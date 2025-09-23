package ru.my

import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Path
import kotlin.io.path.outputStream

// Размер буфера для streaming копирования (8KB по умолчанию)
private const val BUFFER_SIZE = 8192

private fun <R> withBufferedOutput(destination: Path, block: OutputStream.() -> R): R =
    destination.outputStream().buffered().use { output -> output.block() }

fun InputStream.copyToExact(destination: Path, expectedSize: Long): Long {
    val totalRead = withBufferedOutput(destination) {
        copyToWithExpectedSize(this, expectedSize)
    }

    return totalRead
}

fun InputStream.copyToLimited(destination: Path, maxSize: Long): Long {
    val totalRead = withBufferedOutput(destination) {
        copyToWithMaxSize(this, maxSize)
    }

    return totalRead
}

fun InputStream.copyToWithExpectedSize(output: OutputStream, expectedSize: Long): Long =
    copyToWithValidation(
        output = output,
        limit = expectedSize,
        onChunk = { totalRead, bytesRead ->
            if (totalRead + bytesRead > expectedSize) {
                throw IOException("Read overflow: expected $expectedSize, got ${totalRead + bytesRead}")
            }
        },
        onDone = { totalRead ->
            if (totalRead != expectedSize) {
                throw IOException("Incomplete: expected $expectedSize, got $totalRead")
            }
        }
    )

fun InputStream.copyToWithMaxSize(output: OutputStream, maxSize: Long): Long {
    val sanitizedMaxSize = if (maxSize > 0L) maxSize else Long.MAX_VALUE

    return copyToWithValidation(
        output = output,
        limit = sanitizedMaxSize,
        onChunk = { totalRead, bytesRead ->
            if (totalRead + bytesRead > sanitizedMaxSize) {
                throw IOException("Size limit exceeded: $sanitizedMaxSize")
            }
        },
        onDone = { totalRead ->
            // Если мы на лимите — проверим, не осталось ли ещё данных
            if (totalRead == sanitizedMaxSize && read() != -1) {
                throw IOException("File size exceeds limit of $sanitizedMaxSize bytes")
            }
        }
    )
}

private inline fun InputStream.copyToWithValidation(
    output: OutputStream,
    limit: Long,
    crossinline onChunk: (totalRead: Long, nextSize: Int) -> Unit,
    crossinline onDone: (totalRead: Long) -> Unit
): Long {
    val buffer = ByteArray(BUFFER_SIZE)
    var totalRead = 0L

    while (totalRead < limit) {
        val bytesRead = read(buffer)
        if (bytesRead == -1) break

        onChunk(totalRead, bytesRead)

        output.write(buffer, 0, bytesRead)
        totalRead += bytesRead
    }

    onDone(totalRead)

    return totalRead
}
