// This code is largely copied from ring-client-api, but converted from Typescript
// to native Javascript with custom logging for ring-mqtt and some unused code removed.
// Much thanks to @dgreif for the original code which is the basis for this work.

import { FfmpegProcess, reservePorts, RtpSplitter } from '@homebridge/camera-utils'
import { firstValueFrom, ReplaySubject, Subject } from 'rxjs'
import pathToFfmpeg from 'ffmpeg-for-homebridge'
import { concatMap, take } from 'rxjs/operators'
import { RtpPacket } from 'werift'
import { Subscribed } from './subscribed.js'

function getCleanSdp(sdp) {
    return sdp
        .split('\nm=')
        .slice(1)
        .map((section) => 'm=' + section)
        .join('\n')
}

export class StreamingSession extends Subscribed {
    constructor(camera, connection) {
        super()
        this.camera = camera
        this.connection = connection
        this.onCallEnded = new ReplaySubject(1)
        this.onUsingOpus = new ReplaySubject(1)
        this.onVideoRtp = new Subject()
        this.onAudioRtp = new Subject()
        this.audioSplitter = new RtpSplitter()
        this.videoSplitter = new RtpSplitter()
        this.returnAudioSplitter = null   // created lazily in transcodeReturnAudio
        this.returnAudioFf = null
        this.hasEnded = false
        this.bindToConnection(connection)
    }

    bindToConnection(connection) {
        this.addSubscriptions(
            connection.onAudioRtp.subscribe(this.onAudioRtp),
            connection.onVideoRtp.subscribe(this.onVideoRtp),
            connection.onCallAnswered.subscribe((sdp) => {
                this.onUsingOpus.next(sdp.toLocaleLowerCase().includes(' opus/'))
            }),
            connection.onCallEnded.subscribe(() => this.callEnded()))
    }

    async reservePort(bufferPorts = 0) {
        const ports = await reservePorts({ count: bufferPorts + 1 })
        return ports[0]
    }

    get isUsingOpus() {
        return firstValueFrom(this.onUsingOpus)
    }

    async startTranscoding(ffmpegOptions) {
        if (this.hasEnded) {
            return
        }
        const videoPort = await this.reservePort(1)
        const audioPort = await this.reservePort(1)

        const ringSdp = await Promise.race([
            firstValueFrom(this.connection.onCallAnswered),
            firstValueFrom(this.onCallEnded),
        ])

        if (!ringSdp) {
            // Call ended before answered'
            return
        }
        const usingOpus = await this.isUsingOpus

        const ffmpegInputArguments = [
            '-hide_banner',
            '-protocol_whitelist',
            'pipe,udp,rtp,file,crypto',
            // Ring will answer with either opus or pcmu
            ...(usingOpus ? ['-acodec', 'libopus'] : []),
            '-f',
            'sdp',
            ...(ffmpegOptions.input || []),
            '-i',
            'pipe:'
        ]

        const inputSdp = getCleanSdp(ringSdp)
            .replace(/m=audio \d+/, `m=audio ${audioPort}`)
            .replace(/m=video \d+/, `m=video ${videoPort}`)

        const ff = new FfmpegProcess({
            ffmpegArgs: ffmpegInputArguments.concat(
                ...(ffmpegOptions.audio || ['-acodec', 'aac']),
                ...(ffmpegOptions.video || ['-vcodec', 'copy']),
                ...(ffmpegOptions.output || [])),
            ffmpegPath: pathToFfmpeg,
            exitCallback: () => this.callEnded()
        })

        this.addSubscriptions(this.onAudioRtp.pipe(concatMap((rtp) => {
            return this.audioSplitter.send(rtp.serialize(), { port: audioPort })
        })).subscribe())

        this.addSubscriptions(this.onVideoRtp.pipe(concatMap((rtp) => {
            return this.videoSplitter.send(rtp.serialize(), { port: videoPort })
        })).subscribe())

        this.onCallEnded.pipe(take(1)).subscribe(() => ff.stop())

        ff.writeStdin(inputSdp)

        // Request a key frame now that ffmpeg is ready to receive
        this.requestKeyFrame()
    }

    // PIECE 2: encode a talk source and stream it back to the Ring camera speaker.
    // ffmpegInput is an array of ffmpeg input args, e.g. ['-i', '/path/file.wav']
    // or a live source ['-f','s16le','-ar','48k','-ac','2','-i','pipe:'].
    async transcodeReturnAudio(ffmpegInput) {
        if (this.hasEnded || this.returnAudioSplitter) {
            return
        }
        // isUsingOpus resolves only after onCallAnswered (acceptAnswer), so awaiting it
        // also guarantees the sender is negotiated before we push any packet.
        const usingOpus = await this.isUsingOpus
        if (this.hasEnded) {
            return
        }

        // Un-mute the Ring speaker before sending — otherwise return audio is silent.
        // Idempotent; the camera_options message is gated on onCameraConnected internally.
        this.connection.activateCameraSpeaker()

        // RECEIVE bridge: ffmpeg UDP RTP out -> deSerialize -> push to werift sender.
        // Returning null consumes the packet (do not forward it anywhere else).
        this.returnAudioSplitter = new RtpSplitter(({ message }) => {
            if (this.hasEnded) {
                return null
            }
            const rtp = RtpPacket.deSerialize(message)
            this.connection.sendAudioRtp(rtp)
            return null
        })
        const returnPort = await this.returnAudioSplitter.portPromise

        const ff = new FfmpegProcess({
            ffmpegArgs: [
                '-hide_banner',
                '-protocol_whitelist', 'pipe,udp,rtp,file,crypto',
                '-re',
                ...ffmpegInput,
                ...(usingOpus
                    ? ['-acodec', 'libopus', '-application', 'lowdelay',
                       '-frame_duration', '20', '-ar', '48k', '-b:a', '48k',
                       '-bufsize', '192k', '-ac', '2']
                    : ['-acodec', 'pcm_mulaw', '-ar', '8k', '-ac', '1']),
                '-flags', '+global_header',
                '-f', 'rtp',
                `rtp://127.0.0.1:${returnPort}`,
            ],
            ffmpegPath: pathToFfmpeg,
            // Do NOT call callEnded() on return-audio ffmpeg exit; talkback ending
            // must not tear down the main live (receive) stream. Reset the one-shot
            // receive bridge so a subsequent talkback burst (push-to-talk) can start.
            exitCallback: () => {
                this.returnAudioFf = null
                if (this.returnAudioSplitter) {
                    this.returnAudioSplitter.close()
                    this.returnAudioSplitter = null
                }
            },
        })
        this.returnAudioFf = ff
        this.onCallEnded.pipe(take(1)).subscribe(() => ff.stop())
    }

    // ISOLATED TEST HOOK (Piece 2 validation, bypasses go2rtc):
    // inject a local audio file straight into the return path to exercise Ring's speaker
    // on a real device. e.g. session.sendAudioFromFile('/data/test-tone.wav')
    async sendAudioFromFile(filePath) {
        return this.transcodeReturnAudio(['-i', filePath])
    }

    callEnded() {
        if (this.hasEnded) {
            return
        }
        this.hasEnded = true
        this.unsubscribe()
        this.onCallEnded.next()
        this.connection.stop()
        this.audioSplitter.close()
        this.videoSplitter.close()
        if (this.returnAudioFf) {
            this.returnAudioFf.stop()
            this.returnAudioFf = null
        }
        if (this.returnAudioSplitter) {
            this.returnAudioSplitter.close()
            this.returnAudioSplitter = null
        }
    }

    stop() {
        this.callEnded()
    }

    requestKeyFrame() {
        this.connection.requestKeyFrame()
    }
}
