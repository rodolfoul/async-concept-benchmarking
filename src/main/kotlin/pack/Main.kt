package pack

import org.eclipse.jetty.io.EofException
import org.eclipse.jetty.server.Request
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.server.handler.AbstractHandler
import org.eclipse.jetty.server.handler.ContextHandler
import org.eclipse.jetty.util.thread.QueuedThreadPool
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.channels.CompletionHandler
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import javax.servlet.AsyncContext
import javax.servlet.WriteListener
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import kotlin.math.min

fun main() {
    println(ProcessHandle.current().pid())

//    val server = Server(4567)
    val server = Server(QueuedThreadPool(8))
    val connector = ServerConnector(server)
    connector.port = 4567

    server.addConnector(connector)

    val context = ContextHandler()
    context.contextPath = "/img"
    context.handler = TestHandler()
    server.handler = context


    server.start()
}

class TestHandler : AbstractHandler() {
    val rand = Random()
    override fun handle(
        target: String,
        baseRequest: Request,
        request: HttpServletRequest,
        response: HttpServletResponse
    ) {
//        println("Serving request")
//        AsyncHandler(Paths.get("img.jpg"), request.startAsync())
//            .start()

        AsyncFileReader(Paths.get("img.jpg"), request.startAsync()).start()
//        AsyncFileReader(Paths.get("sample.txt"), request.startAsync()).start()
    }
}

val fileReaderThreadPool: ExecutorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())

class AsyncFileReader(path: Path, private val ctx: AsyncContext) : WriteListener {

    private val fileChannel = AsynchronousFileChannel.open(
        path, setOf(StandardOpenOption.READ),
        fileReaderThreadPool
    )
    private val fileSize = fileChannel.size()
    private val outputStream = ctx.response.outputStream
    private val byteQueue = ConcurrentLinkedQueue<ByteArray>()
    private val readingBuffer = ByteBuffer.allocateDirect(40 * 1024)

    private val isReading = AtomicBoolean(true)
    private val isWriting = AtomicBoolean(true)

    private val writeCount = AtomicLong()

    inner class MyCompletionReader : CompletionHandler<Int, ByteBuffer> {

        private val readCount = AtomicLong()

        override fun completed(result: Int, attachment: ByteBuffer) {
            if (!isWriting.get()) {
                finishReading()
                return
            }

            attachment.flip()

            while (attachment.position() != attachment.limit()) {
                val size = min(4 * 1024, attachment.remaining())
                val arr = ByteArray(size)
                attachment.get(arr)

                byteQueue.add(arr)
            }
            attachment.clear()

            val count = readCount.addAndGet(result.toLong())

            try {
                if (count == fileSize) {
                    finishReading()
                    return
                }
            } finally {
                asyncWrite()
            }
            fileChannel.read(attachment, readCount.get(), attachment, this)
        }

        override fun failed(exc: Throwable, attachment: ByteBuffer) {
            exc.printStackTrace()
            finishReading()
        }

        private fun finishReading() {
            isReading.set(false)
            fileChannel.close()
        }
    }

    fun start() {
        outputStream.setWriteListener(this)
        fileChannel.read(readingBuffer, 0, readingBuffer, MyCompletionReader())
    }

    override fun onWritePossible() {
        if (isReading.get()) {
            return
        }

        asyncWrite()
    }

    private fun asyncWrite() {
        if (!isWriting.get()) {
            return
        }

        synchronized(this) {
            while (outputStream.isReady) {
                val value = byteQueue.poll() ?: break
                try {
                    outputStream.write(value)
                    writeCount.addAndGet(value.size.toLong())

                } catch (e: EofException) {
                    finishWriting()
                    return
                }
            }

            if (!isReading.get() && byteQueue.isEmpty()) {
                finishWriting()
            }
        }
    }

    override fun onError(t: Throwable) {
        if (t !is EofException) {
            t.printStackTrace()
        }

        finishWriting()
    }

    private fun finishWriting() {
        if (isWriting.getAndSet(false)) {
            outputStream.close()
            ctx.complete()
        }
    }
}