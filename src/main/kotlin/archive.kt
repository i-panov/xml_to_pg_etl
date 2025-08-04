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
 * @param removeArchiveAfterExtraction удалять ли архив после успешного извлечения
 * @param maxFileSizeBytes максимальный размер извлекаемого файла в байтах
 * @return список путей к извлеченным XML файлам
 */
fun extractArchive(
    archiveFile: Path,
    extractDir: Path,
    xmlFileExtensions: Set<String> = setOf("xml"),
    removeArchiveAfterExtraction: Boolean = false,
    maxFileSizeBytes: Long = MAX_FILE_SIZE
): List<Path> {
    if (!archiveFile.exists()) {
        throw IllegalArgumentException("Archive file not found: $archiveFile")
    }

    if (!archiveFile.isRegularFile()) {
        throw IllegalArgumentException("Archive path is not a regular file: $archiveFile")
    }

    extractDir.createDirectories()
    logger.info("Extracting archive: ${archiveFile.fileName} to $extractDir")

    val extractedFiles = when {
        archiveFile.toString().endsWith(".zip", ignoreCase = true) ->
            extractWithFormat(archiveFile, extractDir, ArchiveStreamFactory.ZIP, xmlFileExtensions, maxFileSizeBytes)
        archiveFile.toString().endsWith(".tar.gz", ignoreCase = true) || archiveFile.toString().endsWith(".tgz", ignoreCase = true) ->
            extractTarGz(archiveFile, extractDir, xmlFileExtensions, maxFileSizeBytes)
        archiveFile.toString().endsWith(".tar.bz2", ignoreCase = true) || archiveFile.toString().endsWith(".tbz2", ignoreCase = true) ->
            extractTarBz2(archiveFile, extractDir, xmlFileExtensions, maxFileSizeBytes)
        archiveFile.toString().endsWith(".tar", ignoreCase = true) ->
            extractWithFormat(archiveFile, extractDir, ArchiveStreamFactory.TAR, xmlFileExtensions, maxFileSizeBytes)
        archiveFile.toString().endsWith(".gz", ignoreCase = true) ->
            extractGzip(archiveFile, extractDir, xmlFileExtensions, maxFileSizeBytes)
        archiveFile.toString().endsWith(".bz2", ignoreCase = true) ->
            extractBzip2(archiveFile, extractDir, xmlFileExtensions, maxFileSizeBytes)
        archiveFile.toString().endsWith(".7z", ignoreCase = true) ->
            extractWithFormat(archiveFile, extractDir, ArchiveStreamFactory.SEVEN_Z, xmlFileExtensions, maxFileSizeBytes)
        else ->
            extractGeneric(archiveFile, extractDir, xmlFileExtensions, maxFileSizeBytes)
    }

    // Удаляем архив после успешного извлечения
    if (removeArchiveAfterExtraction && extractedFiles.isNotEmpty()) {
        if (archiveFile.deleteIfExists()) {
            logger.info("Deleted archive: $archiveFile")
        } else {
            logger.warning("Failed to delete archive: $archiveFile")
        }
    }

    return extractedFiles
}

/**
 * Общая логика извлечения записей из архивного потока
 */
private fun extractEntries(
    archiveStream: ArchiveInputStream<out ArchiveEntry>,
    extractDir: Path,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<Path> {
    val extractedFiles = mutableListOf<Path>()
    var entry: ArchiveEntry? = archiveStream.nextEntry

    while (entry != null) {
        if (!entry.isDirectory && isXmlFile(entry.name, xmlExtensions)) {
            if (entry.size > maxFileSizeBytes) {
                logger.warning("Skipping file ${entry.name}: size ${entry.size} exceeds limit $maxFileSizeBytes")
            } else {
                val outputFile = extractDir.resolve(sanitizeFileName(entry.name))
                outputFile.parent.createDirectories()
                try {
                    streamingCopy(archiveStream, outputFile, entry.size)
                    extractedFiles.add(outputFile)
                    logger.info("Extracted: ${entry.name} (${entry.size} bytes)")
                } catch (e: Exception) {
                    logger.severe("Failed to extract ${entry.name}: ${e.message}")
                    outputFile.deleteIfExists()
                    throw e
                }
            }
        }
        entry = archiveStream.nextEntry
    }

    return extractedFiles
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

private fun extractWithFormat(
    archiveFile: Path,
    extractDir: Path,
    format: String,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<Path> {
    archiveFile.inputStream().use { inputStream ->
        return inputStream.toArchiveInputStream(format).use { archiveStream ->
            extractEntries(archiveStream, extractDir, xmlExtensions, maxFileSizeBytes)
        }
    }
}

private fun extractTarGz(
    archiveFile: Path,
    extractDir: Path,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<Path> {
    return archiveFile.inputStream().use { fis ->
        fis.toCompressorInputStream(CompressorStreamFactory.GZIP).use { gzipStream ->
            gzipStream.toArchiveInputStream(ArchiveStreamFactory.TAR).use { tarStream ->
                extractEntries(tarStream, extractDir, xmlExtensions, maxFileSizeBytes)
            }
        }
    }
}

private fun extractTarBz2(
    archiveFile: Path,
    extractDir: Path,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<Path> {
    return archiveFile.inputStream().use { fis ->
        fis.toCompressorInputStream(CompressorStreamFactory.BZIP2).use { bzip2Stream ->
            bzip2Stream.toArchiveInputStream(ArchiveStreamFactory.TAR).use { tarStream ->
                extractEntries(tarStream, extractDir, xmlExtensions, maxFileSizeBytes)
            }
        }
    }
}

private fun extractGzip(
    archiveFile: Path,
    extractDir: Path,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<Path> {
    return archiveFile.inputStream().use { fis ->
        val outputFileName = archiveFile.fileName.toString().removeSuffix(".gz")

        if (isXmlFile(outputFileName, xmlExtensions)) {
            fis.toCompressorInputStream(CompressorStreamFactory.GZIP).use { gzipStream ->
                val outputFile = extractDir.resolve(sanitizeFileName(outputFileName))
                outputFile.parent.createDirectories()

                try {
                    streamingCopyWithLimit(gzipStream, outputFile, maxFileSizeBytes)
                    logger.info("Extracted: $outputFileName")
                    listOf(outputFile)
                } catch (e: Exception) {
                    logger.severe("Failed to extract $outputFileName: ${e.message}")
                    outputFile.deleteIfExists()
                    throw e
                }
            }
        }
        emptyList()
    }
}

private fun extractBzip2(
    archiveFile: Path,
    extractDir: Path,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<Path> {
    return archiveFile.inputStream().use { fis ->
        val outputFileName = archiveFile.fileName.toString().removeSuffix(".bz2")

        if (isXmlFile(outputFileName, xmlExtensions)) {
            fis.toCompressorInputStream(CompressorStreamFactory.BZIP2).use { bzip2Stream ->
                val outputFile = extractDir.resolve(sanitizeFileName(outputFileName))
                outputFile.parent.createDirectories()

                try {
                    streamingCopyWithLimit(bzip2Stream, outputFile, maxFileSizeBytes)
                    logger.info("Extracted: $outputFileName")
                    listOf(outputFile)
                } catch (e: Exception) {
                    logger.severe("Failed to extract $outputFileName: ${e.message}")
                    outputFile.deleteIfExists()
                    throw e
                }
            }
        }

        emptyList()
    }
}

private fun extractGeneric(
    archiveFile: Path,
    extractDir: Path,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<Path> {
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

            // Восстанавливаем защиту от zip bombs (была в старом коде)
            if (totalRead > size * 2) {
                throw IllegalStateException("File size exceeds expected size by 2x: expected $size, got $totalRead")
            }
        }

        // Проверяем, что файл полностью распакован
        if (totalRead in 1..<size) {
            throw IOException("Incomplete file extraction: expected $size bytes, got $totalRead")
        }

        // Возвращаем логирование, которое было в старом коде
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

private fun sanitizeFileName(fileName: String): String {
    return fileName.replace(Regex("[\\\\/:*?\"<>|]"), "_")
}

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
