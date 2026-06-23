// This code is largely copied from ring-client-api, but converted from Typescript
// to native Javascript with custom logging for ring-mqtt and some unused code removed.
// Much thanks to @dgreif for the original code which is the basis for this work.

import { RTCPeerConnection, RTCRtpCodecParameters, RtpPacket } from 'werift'
import { interval, merge, ReplaySubject, Subject } from 'rxjs'
import { Subscribed } from './subscribed.js'

const ringIceServers = [
    'stun:stun.kinesisvideo.us-east-1.amazonaws.com:443',
    'stun:stun.kinesisvideo.us-east-2.amazonaws.com:443',
    'stun:stun.kinesisvideo.us-west-2.amazonaws.com:443',
    'stun:stun.l.google.com:19302',
    'stun:stun1.l.google.com:19302',
    'stun:stun2.l.google.com:19302',
    'stun:stun3.l.google.com:19302',
    'stun:stun4.l.google.com:19302',
]

export class WeriftPeerConnection extends Subscribed {
    constructor() {
        super()
        this.onAudioRtp = new Subject()
        this.onVideoRtp = new Subject()
        this.onIceCandidate = new Subject()
        this.onConnectionState = new ReplaySubject(1)
        this.onRequestKeyFrame = new Subject()
        const pc = (this.pc = new RTCPeerConnection({
            codecs: {
                audio: [
                    new RTCRtpCodecParameters({
                        mimeType: 'audio/opus',
                        clockRate: 48000,
                        channels: 2,
                    }),
                    new RTCRtpCodecParameters({
                        mimeType: 'audio/PCMU',
                        clockRate: 8000,
                        channels: 1,
                        payloadType: 0,
                    }),
                ],
                video: [
                    new RTCRtpCodecParameters({
                        mimeType: 'video/H264',
                        clockRate: 90000,
                        rtcpFeedback: [
                            { type: 'transport-cc' },
                            { type: 'ccm', parameter: 'fir' },
                            { type: 'nack' },
                            { type: 'nack', parameter: 'pli' },
                            { type: 'goog-remb' },
                        ],
                        parameters: 'packetization-mode=1;profile-level-id=42001f;level-asymmetry-allowed=1',
                    }),
                    new RTCRtpCodecParameters({
                        mimeType: "video/rtx",
                        clockRate: 90000,
                    })
                ],
            },
            iceServers: ringIceServers.map((server) => ({ urls: server })),
            iceTransportPolicy: 'all',
            bundlePolicy: 'disable'
        }))

        const audioTransceiver = pc.addTransceiver('audio', {
            direction: 'sendrecv',
        })
        // Persist so the return-audio (send) path can reach the sender after the
        // answer is negotiated. Direction is already 'sendrecv' so no renegotiation.
        this.audioTransceiver = audioTransceiver

        const videoTransceiver = pc.addTransceiver('video', {
            direction: 'recvonly',
        })

        audioTransceiver.onTrack.subscribe((track) => {
            track.onReceiveRtp.subscribe((rtp) => {
                this.onAudioRtp.next(rtp)
            })
        })

        videoTransceiver.onTrack.subscribe((track) => {
            track.onReceiveRtp.subscribe((rtp) => {
                this.onVideoRtp.next(rtp)
            })
            track.onReceiveRtp.once(() => {
                // debug('received first video packet')
                this.addSubscriptions(merge(this.onRequestKeyFrame, interval(4000)).subscribe(() => {
                    videoTransceiver.receiver
                        .sendRtcpPLI(track.ssrc)
                        .catch()
                }))
                this.requestKeyFrame()
            })
        })

        this.pc.onIceCandidate.subscribe((iceCandidate) => {
            if (iceCandidate) {
                this.onIceCandidate.next(iceCandidate)
            }
        })

        pc.iceConnectionStateChange.subscribe(() => {
            // debug(`iceConnectionStateChange: ${pc.iceConnectionState}`)
            if (pc.iceConnectionState === 'closed') {
                this.onConnectionState.next('closed')
            }
        })

        pc.connectionStateChange.subscribe(() => {
            // debug(`connectionStateChange: ${pc.connectionState}`)
            this.onConnectionState.next(pc.connectionState)
        })
    }

    async createOffer() {
        const offer = await this.pc.createOffer()
        await this.pc.setLocalDescription(offer)
        return offer
    }

    async acceptAnswer(answer) {
        await this.pc.setRemoteDescription(answer)
    }

    addIceCandidate(candidate) {
        return this.pc.addIceCandidate(candidate)
    }

    requestKeyFrame() {
        this.onRequestKeyFrame.next()
    }

    // Push a single return-audio RTP packet to Ring. Accepts a werift RtpPacket or a
    // raw Buffer (deserialized here). werift's RTCRtpSender.sendRtp overwrites ssrc &
    // payloadType and offsets sequenceNumber/timestamp, so do NOT set them on the caller side.
    sendAudioRtp(rtp) {
        if (!this.audioTransceiver || !this.audioTransceiver.sender) {
            return
        }
        const packet = Buffer.isBuffer(rtp) ? RtpPacket.deSerialize(rtp) : rtp
        // sendRtp is async; fire-and-forget but swallow errors (e.g. transport not yet
        // connected) so a dropped packet never crashes the worker.
        this.audioTransceiver.sender.sendRtp(packet).catch(() => {})
    }

    close() {
        this.pc.close().catch()
        this.unsubscribe()
    }
}
