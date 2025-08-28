package ru.my

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.ArchiveInputStream
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.compressors.CompressorInputStream
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.InputStream
import java.nio.file.Path
import kotlin.io.path.*

const val MAX_FILE_SIZE = 1024L * 1024L * 1024L

fun extractArchive(
    archiveFile: Path,
    extractDir: Path,
    maxFileSizeBytes: Long = MAX_FILE_SIZE,
    checkerFileNameForExtract: (String) -> Boolean = { true },
): Flow<Path> {
    if (!archiveFile.exists()) {
        throw IllegalArgumentException("Archive file not found: $archiveFile")
    }

    if (!archiveFile.isRegularFile()) {
        throw IllegalArgumentException("Archive path is not a regular file: $archiveFile")
    }

    extractDir.createDirectories()

    val logger = LoggerFactory.getLogger("ArchiveExtractor")

    fun fileNameEndsWith(sub: String): Boolean = archiveFile.toString().endsWith(sub, ignoreCase = true)

    /**
     * Общая логика извлечения записей из архивного потока
     */
    fun extractEntries(
        archiveStream: ArchiveInputStream<out ArchiveEntry>,
    ): Sequence<Path> = sequence {
        for (entry in archiveStream.iterateEntries()) {
            if (entry.isDirectory || !checkerFileNameForExtract(entry.name)) {
                continue
            }

            if (entry.size != -1L && entry.size > maxFileSizeBytes) {
                logger.warn("Skipping file ${entry.name}: size ${entry.size} exceeds limit $maxFileSizeBytes")
                continue
            }

            val outputFile = extractDir.resolve(sanitizeFileName(entry.name))
            outputFile.parent.createDirectories()

            try {
                val totalRead = if (entry.size != -1L) {
                    archiveStream.copyToExact(outputFile, entry.size)
                } else {
                    archiveStream.copyToLimited(outputFile, maxFileSizeBytes)
                }

                logger.info("Copied $totalRead bytes to ${outputFile.fileName}")
                yield(outputFile)
                logger.info("Extracted: ${entry.name} (${entry.size} bytes)")
            } catch (e: Exception) {
                logger.error("Failed to extract ${entry.name}: ${e.message}")
                outputFile.deleteIfExists()
                throw e
            }
        }
    }

    val inputStream = archiveFile.inputStream().buffered()
    val closeableItems: MutableSet<Closeable> = mutableSetOf(inputStream)

    fun extractInternal(format: String, compressorFormat: String? = null): Sequence<Path> = sequence {
        if (compressorFormat.isNullOrBlank()) {
            val archiveStream = inputStream.toArchiveInputStream(format)
            closeableItems.add(archiveStream)
            return@sequence yieldAll(extractEntries(archiveStream))
        }

        val compressedStream = inputStream.toCompressorInputStream(compressorFormat)
        closeableItems.add(compressedStream)
        val archiveStream = compressedStream.toArchiveInputStream(format)
        closeableItems.add(archiveStream)
        yieldAll(extractEntries(archiveStream))
    }

    fun extractCompressed(suffixToRemove: String, compressorFormat: String): Sequence<Path> = sequence {
        val compressedStream = inputStream.toCompressorInputStream(compressorFormat)
        closeableItems.add(compressedStream)
        val outputFileName = archiveFile.fileName.toString().removeSuffix(suffixToRemove)

        if (checkerFileNameForExtract(outputFileName)) {
            val outputFile = extractDir.resolve(sanitizeFileName(outputFileName))
            outputFile.parent.createDirectories()

            try {
                val totalRead = compressedStream.copyToLimited(outputFile, maxFileSizeBytes)
                logger.info("Copied $totalRead bytes to ${outputFile.fileName}")
                yield(outputFile)
                logger.info("Extracted: $outputFileName")
            } catch (e: Exception) {
                logger.error("Failed to extract $outputFileName: ${e.message}")
                outputFile.deleteIfExists()
                throw e
            }
        }
    }

    return flow {
        try {
            logger.info("Extracting archive: ${archiveFile.fileName} to $extractDir")

            emitAll(when {
                fileNameEndsWith(".zip") -> extractInternal(ArchiveStreamFactory.ZIP)
                fileNameEndsWith(".tar") -> extractInternal(ArchiveStreamFactory.TAR)
                fileNameEndsWith(".7z") -> extractInternal(ArchiveStreamFactory.SEVEN_Z)
                fileNameEndsWith(".tar.gz") || fileNameEndsWith(".tgz") -> extractInternal(
                    ArchiveStreamFactory.TAR, CompressorStreamFactory.GZIP,
                )
                fileNameEndsWith(".tar.bz2") || fileNameEndsWith(".tbz2") -> extractInternal(
                    ArchiveStreamFactory.TAR, CompressorStreamFactory.BZIP2,
                )
                fileNameEndsWith(".gz") -> extractCompressed(".gz", CompressorStreamFactory.GZIP)
                fileNameEndsWith(".bz2") -> extractCompressed(".bz2", CompressorStreamFactory.BZIP2)
                else -> run {
                    try {
                        val archiveStream = inputStream.toArchiveInputStream()
                        closeableItems.add(archiveStream)
                        extractEntries(archiveStream)
                    } catch (e: Exception) {
                        logger.error("Failed to extract archive ${archiveFile.fileName}: ${e.message}")
                        throw IllegalArgumentException("Unsupported archive format: ${archiveFile.fileName}", e)
                    }
                }
            }.asFlow())
        } finally {
            closeableItems.forEach { it.close() }
        }
    }
}

// --------------------------------------------------------------------------------------
// HELPERS
// --------------------------------------------------------------------------------------

private fun sanitizeFileName(fileName: String) = fileName.replace(Regex("[\\\\/:*?\"<>|]"), "_")

private val ARCHIVE_EXTENSIONS = setOf(".zip", ".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".gz", ".bz2", ".7z")

fun isArchive(path: String): Boolean {
    val lowerPath = path.lowercase()
    return ARCHIVE_EXTENSIONS.any { lowerPath.endsWith(it) }
}

// --------------------------------------------------------------------------------------
// EXTENSIONS
// --------------------------------------------------------------------------------------

fun ArchiveInputStream<out ArchiveEntry>.iterateEntries(): Sequence<ArchiveEntry> = sequence {
    var entry: ArchiveEntry? = nextEntry

    while (entry != null) {
        yield(entry)
        entry = nextEntry
    }
}

fun InputStream.toArchiveInputStream(format: String = ""): ArchiveInputStream<ArchiveEntry> {
    val factory = ArchiveStreamFactory()

    return if (format.isBlank()) {
        factory.createArchiveInputStream(this)
    } else {
        factory.createArchiveInputStream(format, this)
    }
}

fun InputStream.toCompressorInputStream(format: String): CompressorInputStream {
    val compressorFactory = CompressorStreamFactory()
    return compressorFactory.createCompressorInputStream(format, this)
}
