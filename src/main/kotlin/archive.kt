package ru.my

import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.ArchiveInputStream
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.compressors.CompressorInputStream
import org.apache.commons.compress.compressors.CompressorStreamFactory
import java.io.IOException
import java.io.InputStream
import java.nio.file.Path
import java.util.logging.Logger
import kotlin.io.path.*

private val logger = Logger.getLogger("ArchiveExtractor")

// Размер буфера для streaming копирования (8KB по умолчанию)
private const val BUFFER_SIZE = 8192
// Максимальный размер файла для извлечения (например, 1GB)
private const val MAX_FILE_SIZE = 1024L * 1024L * 1024L

/**
 * Универсальная функция для извлечения архивов различных форматов с streaming обработкой
 * Поддерживает: ZIP, TAR, TAR.GZ, TAR.BZ2, 7Z, GZIP, BZIP2 и др.
 *
 * @param archiveFile путь к архиву
 * @param extractDir папка для извлечения
 * @param xmlFileExtensions расширения XML файлов для извлечения
 * @param maxFileSizeBytes максимальный размер извлекаемого файла в байтах
 * @return список путей к извлеченным XML файлам
 */
fun extractArchive(
    archiveFile: Path,
    extractDir: Path,
    xmlFileExtensions: Set<String> = setOf("xml"),
    maxFileSizeBytes: Long = MAX_FILE_SIZE
): Sequence<Path> = sequence {
    // TODO: протестировать разархивацию, у меня не сработала на zip архиве.
    // Просто папка создалась пустая и вышло с сообщением что нет файлов.

    if (!archiveFile.exists()) {
        throw IllegalArgumentException("Archive file not found: $archiveFile")
    }

    if (!archiveFile.isRegularFile()) {
        throw IllegalArgumentException("Archive path is not a regular file: $archiveFile")
    }

    extractDir.createDirectories()
    logger.info("Extracting archive: ${archiveFile.fileName} to $extractDir")
    val fileName = archiveFile.toString()
    fun fileNameEndsWith(sub: String): Boolean = fileName.endsWith(sub, ignoreCase = true)

    fun extractInternalLocal(format: String) = extractInternal(
        archiveFile = archiveFile,
        extractDir = extractDir,
        format = format,
        xmlExtensions = xmlFileExtensions,
        maxFileSizeBytes = maxFileSizeBytes,
    )

    val extractedFiles = when {
        fileNameEndsWith(".zip") -> extractInternalLocal(ArchiveStreamFactory.ZIP)
        fileNameEndsWith(".tar") -> extractInternalLocal(ArchiveStreamFactory.TAR)
        fileNameEndsWith(".7z") -> extractInternalLocal(ArchiveStreamFactory.SEVEN_Z)
        fileNameEndsWith(".tar.gz") || fileNameEndsWith(".tgz") -> extractInternal(
            archiveFile = archiveFile,
            extractDir = extractDir,
            format = ArchiveStreamFactory.TAR,
            xmlExtensions = xmlFileExtensions,
            maxFileSizeBytes = maxFileSizeBytes,
            compressorFormat = CompressorStreamFactory.GZIP,
        )
        fileNameEndsWith(".tar.bz2") || fileNameEndsWith(".tbz2") -> extractInternal(
            archiveFile = archiveFile,
            extractDir = extractDir,
            format = ArchiveStreamFactory.TAR,
            xmlExtensions = xmlFileExtensions,
            maxFileSizeBytes = maxFileSizeBytes,
            compressorFormat = CompressorStreamFactory.BZIP2,
        )
        fileNameEndsWith(".gz") -> extractCompressedFile(
            archiveFile = archiveFile,
            extractDir = extractDir,
            xmlExtensions = xmlFileExtensions,
            maxFileSizeBytes = maxFileSizeBytes,
            suffixToRemove = ".gz",
            compressorFormat = CompressorStreamFactory.GZIP
        )
        fileNameEndsWith(".bz2") -> extractCompressedFile(
            archiveFile = archiveFile,
            extractDir = extractDir,
            xmlExtensions = xmlFileExtensions,
            maxFileSizeBytes = maxFileSizeBytes,
            suffixToRemove = ".bz2",
            compressorFormat = CompressorStreamFactory.BZIP2
        )
        else -> extractGeneric(
            archiveFile = archiveFile,
            extractDir = extractDir,
            xmlExtensions = xmlFileExtensions,
            maxFileSizeBytes = maxFileSizeBytes,
        )
    }

    yieldAll(extractedFiles)
}

fun ArchiveInputStream<out ArchiveEntry>.iterateEntries(): Sequence<ArchiveEntry> = sequence {
    var entry: ArchiveEntry? = nextEntry

    while (entry != null) {
        yield(entry)
        entry = nextEntry
    }
}

/**
 * Общая логика извлечения записей из архивного потока
 */
private fun extractEntries(
    archiveStream: ArchiveInputStream<out ArchiveEntry>,
    extractDir: Path,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): Sequence<Path> = sequence {
    for (entry in archiveStream.iterateEntries()) {
        if (entry.isDirectory || !isXmlFile(entry.name, xmlExtensions)) {
            continue
        }

        if (entry.size > maxFileSizeBytes) {
            logger.warning("Skipping file ${entry.name}: size ${entry.size} exceeds limit $maxFileSizeBytes")
            continue
        }

        val outputFile = extractDir.resolve(sanitizeFileName(entry.name))
        outputFile.parent.createDirectories()

        try {
            streamingCopy(archiveStream, outputFile, entry.size)
            yield(outputFile)
            logger.info("Extracted: ${entry.name} (${entry.size} bytes)")
        } catch (e: Exception) {
            logger.severe("Failed to extract ${entry.name}: ${e.message}")
            outputFile.deleteIfExists()
            throw e
        }
    }
}

private fun InputStream.toArchiveInputStream(format: String? = null): ArchiveInputStream<ArchiveEntry> {
    val factory = ArchiveStreamFactory()

    if (format == null) {
        return factory.createArchiveInputStream(this)
    }

    return factory.createArchiveInputStream(format, this)
}

private fun InputStream.toCompressorInputStream(format: String): CompressorInputStream {
    val compressorFactory = CompressorStreamFactory()
    return compressorFactory.createCompressorInputStream(format, this)
}

private fun extractInternal(
    archiveFile: Path,
    extractDir: Path,
    format: String,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long,
    compressorFormat: String? = null,
): Sequence<Path> = archiveFile.inputStream().use { inputStream ->
    if (compressorFormat != null) {
        return@use inputStream.toCompressorInputStream(compressorFormat).use { compressedStream ->
            compressedStream.toArchiveInputStream(format).use { archiveStream ->
                extractEntries(archiveStream, extractDir, xmlExtensions, maxFileSizeBytes)
            }
        }
    }

    inputStream.toArchiveInputStream(format).use { archiveStream ->
        extractEntries(archiveStream, extractDir, xmlExtensions, maxFileSizeBytes)
    }
}

private fun extractCompressedFile(
    archiveFile: Path,
    extractDir: Path,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long,
    suffixToRemove: String,
    compressorFormat: String
): Sequence<Path> = sequence {
    archiveFile.inputStream().use { fis ->
        fis.toCompressorInputStream(compressorFormat).use { compressedStream ->
            val outputFileName = archiveFile.fileName.toString().removeSuffix(suffixToRemove)

            if (isXmlFile(outputFileName, xmlExtensions)) {
                val outputFile = extractDir.resolve(sanitizeFileName(outputFileName))
                outputFile.parent.createDirectories()

                try {
                    streamingCopyWithLimit(compressedStream, outputFile, maxFileSizeBytes)
                    logger.info("Extracted: $outputFileName")
                    yield(outputFile)
                } catch (e: Exception) {
                    logger.severe("Failed to extract $outputFileName: ${e.message}")
                    outputFile.deleteIfExists()
                    throw e
                }
            }
        }
    }
}

private fun extractGeneric(
    archiveFile: Path,
    extractDir: Path,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): Sequence<Path> {
    try {
        return archiveFile.inputStream().use { fis ->
            fis.toArchiveInputStream().use { archiveStream ->
                extractEntries(archiveStream, extractDir, xmlExtensions, maxFileSizeBytes)
            }
        }
    } catch (e: Exception) {
        logger.severe("Failed to extract archive ${archiveFile.fileName}: ${e.message}")
        throw IllegalArgumentException("Unsupported archive format: ${archiveFile.fileName}", e)
    }
}

// Вспомогательные функции для работы с файлами
private fun streamingCopy(source: InputStream, destination: Path, size: Long) {
    destination.outputStream().use { outputStream ->
        val buffer = ByteArray(BUFFER_SIZE)
        var totalRead = 0L
        var bytesRead: Int

        while (totalRead < size) {
            bytesRead = source.read(buffer)

            if (bytesRead == -1) break

            outputStream.write(buffer, 0, bytesRead)
            totalRead += bytesRead

            if (totalRead > size * 2) {
                throw IllegalStateException("File size exceeds expected size by 2x: expected $size, got $totalRead")
            }
        }

        // Проверяем, что файл полностью распакован
        if (totalRead in 1..<size) {
            throw IOException("Incomplete file extraction: expected $size bytes, got $totalRead")
        }

        logger.fine("Copied $totalRead bytes to ${destination.fileName}")
    }
}

private fun streamingCopyWithLimit(source: InputStream, destination: Path, maxBytes: Long) {
    destination.outputStream().use { outputStream ->
        val buffer = ByteArray(BUFFER_SIZE)
        var totalRead = 0L
        var bytesRead = 0 // Инициализируем переменную

        while (totalRead < maxBytes) {
            bytesRead = source.read(buffer)
            if (bytesRead == -1) break

            // Проверяем, не превысит ли запись лимит
            if (totalRead + bytesRead > maxBytes) {
                throw IOException("File size exceeds limit of $maxBytes bytes")
            }

            outputStream.write(buffer, 0, bytesRead)
            totalRead += bytesRead
        }

        // Если файл не закончился, но мы достигли лимита, это ошибка
        if (bytesRead != -1) {
            throw IOException("File size exceeds limit of $maxBytes bytes")
        }

        // Возвращаем логирование
        logger.fine("Copied $totalRead bytes to ${destination.fileName}")
    }
}

private fun isXmlFile(fileName: String, extensions: Set<String>): Boolean {
    val ext = fileName.substringAfterLast('.', "").lowercase()
    return extensions.any { it.equals(ext, ignoreCase = true) }
}

private fun sanitizeFileName(fileName: String) = fileName.replace(Regex("[\\\\/:*?\"<>|]"), "_")

fun isArchive(path: String): Boolean {
    val lowerPath = path.lowercase()
    return lowerPath.endsWith(".zip") ||
            lowerPath.endsWith(".tar") ||
            lowerPath.endsWith(".tar.gz") ||
            lowerPath.endsWith(".tgz") ||
            lowerPath.endsWith(".tar.bz2") ||
            lowerPath.endsWith(".tbz2") ||
            lowerPath.endsWith(".gz") ||
            lowerPath.endsWith(".bz2") ||
            lowerPath.endsWith(".7z")
}
