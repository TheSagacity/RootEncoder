package com.pedro.common

import com.pedro.common.frame.MediaFrame
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.max

class StreamBlockingQueue(var capacity: Int) {

    private val queue = PriorityBlockingQueue<MediaFrame>(capacity) { p0, p1 ->
        p0.info.timestamp.compare(p1.info.timestamp)
    }
    private var cacheQueue = PriorityBlockingQueue<MediaFrame>(200) { p0, p1 ->
        p0.info.timestamp.compare(p1.info.timestamp)
    }
    private var cacheTimeFilled = AtomicBoolean(false)
    private var cacheTime = 0L
    private var startTs = 0L

    fun trySend(item: MediaFrame): Boolean {
        // PriorityBlockingQueue is unbounded by design - without this check the queue
        // grows forever under network congestion instead of dropping frames, so the
        // backlog never sheds and everything (including audio behind it) falls further
        // and further behind wall-clock instead of staying near-realtime.
        if (queue.size >= capacity) return false
        if (cacheTime > 0 && !cacheTimeFilled.get()) {
            if (startTs == 0L) startTs = TimeUtils.getCurrentTimeMillis()
            val t = TimeUtils.getCurrentTimeMillis() - startTs
            if (t >= cacheTime) cacheTimeFilled.set(true)
        }
        return try {
            if (cacheTime > 0) {
                cacheQueue.add(item)
                if (cacheTimeFilled.get()) queue.add(cacheQueue.take())
            } else queue.add(item)
            return true
        } catch (e: IllegalStateException) {
            false
        }
    }

    fun take(): MediaFrame {
        return queue.take()
    }

    /**
     * Non-blocking. Removes and returns the earliest audio frame in the queue,
     * even if video frames with earlier timestamps are queued ahead of it.
     * Under network backpressure the queue backlogs with video (video bytes >> audio bytes),
     * so newly captured audio always has a later timestamp than the backlogged video and
     * would otherwise wait behind the whole video backlog. Audio frames are never
     * reordered relative to other audio frames, so the audio chunk stream timestamps
     * stay monotonic.
     */
    fun pollAudio(): MediaFrame? {
        // fast path: head is already audio
        val head = queue.poll() ?: return null
        if (head.type == MediaFrame.Type.AUDIO) return head
        queue.add(head) // wasn't audio, put back - priority queue reorders correctly
        // slow path: find the earliest audio frame behind the video backlog
        var candidate: MediaFrame? = null
        for (frame in queue) {
            if (frame.type == MediaFrame.Type.AUDIO &&
                (candidate == null || frame.info.timestamp < candidate.info.timestamp)) {
                candidate = frame
            }
        }
        val audio = candidate ?: return null
        return if (queue.remove(audio)) audio else null
    }

    fun remainingCapacity(): Int = max(0, capacity - queue.size)

    fun drainTo(destiny: StreamBlockingQueue) {
        queue.drainTo(destiny.queue)
        cacheQueue.drainTo(destiny.cacheQueue)
    }

    fun clear() {
        queue.clear()
        cacheQueue.clear()
        startTs = 0L
        cacheTimeFilled.set(false)
    }

    fun setCacheTime(cache: Long) {
        cacheTime = cache
        cacheQueue = PriorityBlockingQueue<MediaFrame>((cache / 5).toInt()) { p0, p1 ->
            p0.info.timestamp.compare(p1.info.timestamp)
        }
    }

    fun getSize() = queue.size
}