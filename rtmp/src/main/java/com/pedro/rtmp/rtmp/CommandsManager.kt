/*
 * Copyright (C) 2024 pedroSG94.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pedro.rtmp.rtmp

import android.util.Log
import com.pedro.common.AudioCodec
import com.pedro.common.TimeUtils
import com.pedro.common.VideoCodec
import com.pedro.rtmp.flv.FlvPacket
import com.pedro.rtmp.rtmp.chunk.ChunkType
import com.pedro.rtmp.rtmp.message.Acknowledgement
import com.pedro.rtmp.rtmp.message.Audio
import com.pedro.rtmp.rtmp.message.BasicHeader
import com.pedro.rtmp.rtmp.message.RtmpMessage
import com.pedro.rtmp.rtmp.message.SetChunkSize
import com.pedro.rtmp.rtmp.message.Video
import com.pedro.rtmp.rtmp.message.WindowAcknowledgementSize
import com.pedro.rtmp.rtmp.message.control.Event
import com.pedro.rtmp.rtmp.message.control.Type
import com.pedro.rtmp.rtmp.message.control.UserControl
import com.pedro.rtmp.utils.CommandSessionHistory
import com.pedro.rtmp.utils.RtmpConfig
import com.pedro.rtmp.utils.socket.RtmpSocket
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.IOException

/**
 * Created by pedro on 21/04/21.
 */
abstract class CommandsManager {

  protected val TAG = "CommandsManager"

  val sessionHistory = CommandSessionHistory()
  var timestamp = 0
  protected var commandId = 0
  var streamId = 0
  var host = ""
  var port = 1935
  var appName = ""
  var streamName = ""
  var tcUrl = ""
  var flashVersion: String = "FMLE/3.0 (compatible; Lavf57.56.101)"
  var user: String? = null
  var password: String? = null
  var onAuth = false
  var startTs = 0L
  var readChunkSize = RtmpConfig.DEFAULT_CHUNK_SIZE
  var audioDisabled = false
  var videoDisabled = false
  var customAmfObject: Map<String, Any> = emptyMap()
  private var bytesRead = 0
  private var acknowledgementSequence = 0

  protected var width = 640
  protected var height = 480
  var fps = 30
  protected var sampleRate = 44100
  protected var isStereo = true
  var videoCodec = VideoCodec.H264
  var audioCodec = AudioCodec.AAC
  //Avoid write a packet in middle of other.
  private val writeSync = Mutex(locked = false)

  fun setVideoResolution(width: Int, height: Int) {
    this.width = width
    this.height = height
  }

  fun setAudioInfo(sampleRate: Int, isStereo: Boolean) {
    this.sampleRate = sampleRate
    this.isStereo = isStereo
  }

  fun setAuth(user: String?, password: String?) {
    this.user = user
    this.password = password
  }

  protected fun getCurrentTimestamp(): Int {
    return (TimeUtils.getCurrentTimeMillis() / 1000 - timestamp).toInt()
  }

  @Throws(IOException::class)
  suspend fun sendChunkSize(socket: RtmpSocket) {
    writeSync.withLock {
      if (RtmpConfig.writeChunkSize != RtmpConfig.DEFAULT_CHUNK_SIZE) {
        val chunkSize = SetChunkSize(RtmpConfig.writeChunkSize)
        chunkSize.header.timeStamp = getCurrentTimestamp()
        chunkSize.header.messageStreamId = streamId
        chunkSize.writeHeader(socket)
        chunkSize.writeBody(socket)
        socket.flush()
        Log.i(TAG, "send $chunkSize")
      } else {
        Log.i(TAG, "using default write chunk size ${RtmpConfig.DEFAULT_CHUNK_SIZE}")
      }
    }
  }

  @Throws(IOException::class)
  suspend fun sendConnect(auth: String, socket: RtmpSocket) {
    writeSync.withLock {
      sendConnectImp(auth, socket)
      socket.flush()
    }
  }

  @Throws(IOException::class)
  suspend fun createStream(socket: RtmpSocket) {
    writeSync.withLock {
      createStreamImp(socket)
      socket.flush()
    }
  }

  @Throws(IOException::class)
  suspend fun readMessageResponse(socket: RtmpSocket): RtmpMessage {
    val message = RtmpMessage.getRtmpMessage(socket, readChunkSize, sessionHistory)
    sessionHistory.setReadHeader(message.header)
    Log.i(TAG, "read $message")
    bytesRead += message.header.getPacketLength()
    return message
  }

  @Throws(IOException::class)
  suspend fun sendMetadata(socket: RtmpSocket) {
    writeSync.withLock {
      sendMetadataImp(socket)
      socket.flush()
    }
  }

  @Throws(IOException::class)
  suspend fun sendPublish(socket: RtmpSocket) {
    writeSync.withLock {
      sendPublishImp(socket)
      socket.flush()
    }
  }

  @Throws(IOException::class)
  suspend fun sendWindowAcknowledgementSize(socket: RtmpSocket) {
    writeSync.withLock {
      val windowAcknowledgementSize = WindowAcknowledgementSize(RtmpConfig.acknowledgementWindowSize, getCurrentTimestamp())
      windowAcknowledgementSize.writeHeader(socket)
      windowAcknowledgementSize.writeBody(socket)
      socket.flush()
    }
  }

  suspend fun sendPong(event: Event, socket: RtmpSocket) {
    writeSync.withLock {
      val pong = UserControl(Type.PONG_REPLY, event)
      pong.writeHeader(socket)
      pong.writeBody(socket)
      socket.flush()
      Log.i(TAG, "send pong")
    }
  }

  @Throws(IOException::class)
  suspend fun sendClose(socket: RtmpSocket) {
    writeSync.withLock {
      sendCloseImp(socket)
      socket.flush()
    }
  }

  suspend fun checkAndSendAcknowledgement(socket: RtmpSocket) {
    writeSync.withLock {
      if (bytesRead >= RtmpConfig.acknowledgementWindowSize) {
        acknowledgementSequence += bytesRead
        bytesRead -= RtmpConfig.acknowledgementWindowSize
        val acknowledgement = Acknowledgement(acknowledgementSequence)
        acknowledgement.writeHeader(socket)
        acknowledgement.writeBody(socket)
        socket.flush()
        Log.i(TAG, "send $acknowledgement")
      }
    }
  }

  /**
   * @param audioSupplier non-blocking poll for a pending audio frame, already FLV-encoded.
   * Checked between each RTMP chunk of the video body so a ready audio packet doesn't
   * wait behind a whole video frame write (which on a slow/congested uplink can take
   * hundreds of ms and would otherwise starve audio, causing audible crackle/gaps).
   */
  @Throws(IOException::class)
  suspend fun sendVideoPacket(
    flvPacket: FlvPacket, socket: RtmpSocket,
    audioSupplier: (suspend () -> FlvPacket?)? = null,
    onAudioSent: ((Int) -> Unit)? = null
  ): Int {
    writeSync.withLock {
      val video = Video(flvPacket, streamId)
      video.writeHeader(socket)
      if (audioSupplier != null) {
        writeBodyInterleaved(video, socket, audioSupplier, onAudioSent)
      } else {
        video.writeBody(socket)
      }
      socket.flush(true)
      return video.header.getPacketLength() //get packet size with header included to calculate bps
    }
  }

  /**
   * Same chunking as RtmpMessage.writeBody, but after each chunk gives audio a chance
   * to jump the queue and go out on the wire immediately. Must be called while already
   * holding writeSync (caller's responsibility) since it writes directly to the socket.
   */
  @Throws(IOException::class)
  private suspend fun writeBodyInterleaved(
    video: RtmpMessage, socket: RtmpSocket,
    audioSupplier: suspend () -> FlvPacket?,
    onAudioSent: ((Int) -> Unit)?
  ) {
    val chunkSize = RtmpConfig.writeChunkSize
    val bytes = video.storeBody()
    var pos = 0
    var length = video.getSize()

    while (length > chunkSize) {
      socket.write(bytes, pos, chunkSize)
      length -= chunkSize
      pos += chunkSize

      val pendingAudio = audioSupplier()
      if (pendingAudio != null) {
        val audio = Audio(pendingAudio, streamId)
        audio.writeHeader(socket)
        audio.writeBody(socket)
        // socket.write() above only appends to a local (non-flushing) write buffer.
        // Without an actual flush here, this audio packet stays queued behind the
        // rest of the video frame's chunks and only reaches the wire together with
        // them in the single flush(true) call after this loop — i.e. no real
        // interleaving happens on the network, only in this function's bytecode.
        // Flush now so audio is genuinely sent immediately, mid video-frame.
        socket.flush(false)
        onAudioSent?.invoke(audio.header.getPacketLength())
      }

      video.header.writeHeader(BasicHeader(ChunkType.TYPE_3, video.header.basicHeader.chunkStreamId), socket)
    }
    socket.write(bytes, pos, length)
  }

  @Throws(IOException::class)
  suspend fun sendAudioPacket(flvPacket: FlvPacket, socket: RtmpSocket): Int {
    writeSync.withLock {
      val audio = Audio(flvPacket, streamId)
      audio.writeHeader(socket)
      audio.writeBody(socket)
      socket.flush(true)
      return audio.header.getPacketLength() //get packet size with header included to calculate bps
    }
  }

  abstract suspend fun sendConnectImp(auth: String, socket: RtmpSocket)
  abstract suspend fun createStreamImp(socket: RtmpSocket)
  abstract suspend fun sendMetadataImp(socket: RtmpSocket)
  abstract suspend fun sendPublishImp(socket: RtmpSocket)
  abstract suspend fun sendCloseImp(socket: RtmpSocket)

  fun reset() {
    startTs = 0
    timestamp = 0
    streamId = 0
    commandId = 0
    readChunkSize = RtmpConfig.DEFAULT_CHUNK_SIZE
    sessionHistory.reset()
    acknowledgementSequence = 0
    bytesRead = 0
  }
}
