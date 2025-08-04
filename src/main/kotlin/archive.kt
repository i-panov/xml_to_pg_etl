package ru.my

import kotlin.io.path.*
import java.nio.file.*
import java.util.logging.Logger
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.ArchiveInputStream
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.compressors.CompressorStreamFactory
import java.io.IOException
import java.io.InputStream
import java.nio.file.Files.createDirectories

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

private fun extractWithFormat(
    archiveFile: Path,
    extractDir: Path,
    format: String,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<Path> {
    val extractedFiles = mutableListOf<Path>()

    archiveFile.inputStream().use { inputStream ->
        val factory = ArchiveStreamFactory()
        val archiveStream: ArchiveInputStream<out ArchiveEntry> = factory.createArchiveInputStream(format, inputStream)

        var entry: ArchiveEntry? = archiveStream.nextEntry
        while (entry != null) {
            if (!entry.isDirectory && isXmlFile(entry.name, xmlExtensions)) {
                // Проверяем размер файла перед извлечением
                if (entry.size > maxFileSizeBytes) {
                    logger.warning("Skipping file ${entry.name}: size ${entry.size} exceeds limit $maxFileSizeBytes")
                } else {
                    val outputFile = extractDir.resolve(sanitizeFileName(entry.name))
                    createDirectories(outputFile.parent)

                    try {
                        // Streaming копирование с буфером
                        streamingCopy(archiveStream, outputFile, entry.size)
                        extractedFiles.add(outputFile)
                        logger.info("Extracted: ${entry.name} (${entry.size} bytes)")
                    } catch (e: Exception) {
                        logger.severe("Failed to extract ${entry.name}: ${e.message}")
                        // Удаляем частично созданный файл
                        outputFile.deleteIfExists()
                        throw e
                    }
                }
            }
            entry = archiveStream.nextEntry
        }
        archiveStream.close()
    }

    return extractedFiles
}

private fun extractTarGz(
    archiveFile: Path,
    extractDir: Path,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<Path> {
    val extractedFiles = mutableListOf<Path>()

    Files.newInputStream(archiveFile).use { fis ->
        val compressorFactory = CompressorStreamFactory()
        val gzipStream = compressorFactory.createCompressorInputStream(CompressorStreamFactory.GZIP, fis)

        val archiveFactory = ArchiveStreamFactory()
        val tarStream: ArchiveInputStream<out ArchiveEntry> = archiveFactory.createArchiveInputStream(ArchiveStreamFactory.TAR, gzipStream)

        var entry: ArchiveEntry? = tarStream.nextEntry
        while (entry != null) {
            if (!entry.isDirectory && isXmlFile(entry.name, xmlExtensions)) {
                if (entry.size > maxFileSizeBytes) {
                    logger.warning("Skipping file ${entry.name}: size ${entry.size} exceeds limit $maxFileSizeBytes")
                } else {
                    val outputFile = extractDir.resolve(sanitizeFileName(entry.name))
                    createDirectories(outputFile.parent)

                    try {
                        streamingCopy(tarStream, outputFile, entry.size)
                        extractedFiles.add(outputFile)
                        logger.info("Extracted: ${entry.name} (${entry.size} bytes)")
                    } catch (e: Exception) {
                        logger.severe("Failed to extract ${entry.name}: ${e.message}")
                        outputFile.deleteIfExists()
                        throw e
                    }
                }
            }
            entry = tarStream.nextEntry
        }
        tarStream.close()
        gzipStream.close()
    }

    return extractedFiles
}

private fun extractTarBz2(
    archiveFile: Path,
    extractDir: Path,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<Path> {
    val extractedFiles = mutableListOf<Path>()

    Files.newInputStream(archiveFile).use { fis ->
        val compressorFactory = CompressorStreamFactory()
        val bzip2Stream = compressorFactory.createCompressorInputStream(CompressorStreamFactory.BZIP2, fis)

        val archiveFactory = ArchiveStreamFactory()
        val tarStream: ArchiveInputStream<out ArchiveEntry> = archiveFactory.createArchiveInputStream(ArchiveStreamFactory.TAR, bzip2Stream)

        var entry: ArchiveEntry? = tarStream.nextEntry
        while (entry != null) {
            if (!entry.isDirectory && isXmlFile(entry.name, xmlExtensions)) {
                if (entry.size > maxFileSizeBytes) {
                    logger.warning("Skipping file ${entry.name}: size ${entry.size} exceeds limit $maxFileSizeBytes")
                } else {
                    val outputFile = extractDir.resolve(sanitizeFileName(entry.name))
                    createDirectories(outputFile.parent)

                    try {
                        streamingCopy(tarStream, outputFile, entry.size)
                        extractedFiles.add(outputFile)
                        logger.info("Extracted: ${entry.name} (${entry.size} bytes)")
                    } catch (e: Exception) {
                        logger.severe("Failed to extract ${entry.name}: ${e.message}")
                        outputFile.deleteIfExists()
                        throw e
                    }
                }
            }
            entry = tarStream.nextEntry
        }
        tarStream.close()
        bzip2Stream.close()
    }

    return extractedFiles
}

private fun extractGzip(
    archiveFile: Path,
    extractDir: Path,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<Path> {
    val extractedFiles = mutableListOf<Path>()

    Files.newInputStream(archiveFile).use { fis ->
        val compressorFactory = CompressorStreamFactory()
        val gzipStream = compressorFactory.createCompressorInputStream(CompressorStreamFactory.GZIP, fis)
        val outputFileName = archiveFile.fileName.toString().removeSuffix(".gz")

        if (isXmlFile(outputFileName, xmlExtensions)) {
            val outputFile = extractDir.resolve(sanitizeFileName(outputFileName))
            createDirectories(outputFile.parent)

            try {
                // Для GZIP файлов размер неизвестен заранее, поэтому используем ограниченный поток
                streamingCopyWithLimit(gzipStream, outputFile, maxFileSizeBytes)
                extractedFiles.add(outputFile)
                logger.info("Extracted: $outputFileName")
            } catch (e: Exception) {
                logger.severe("Failed to extract $outputFileName: ${e.message}")
                outputFile.deleteIfExists()
                throw e
            }
        }
        gzipStream.close()
    }

    return extractedFiles
}

private fun extractBzip2(
    archiveFile: Path,
    extractDir: Path,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<Path> {
    val extractedFiles = mutableListOf<Path>()

    archiveFile.inputStream().use { fis ->
        val compressorFactory = CompressorStreamFactory()
        val bzip2Stream = compressorFactory.createCompressorInputStream(CompressorStreamFactory.BZIP2, fis)
        val outputFileName = archiveFile.fileName.toString().removeSuffix(".bz2")

        if (isXmlFile(outputFileName, xmlExtensions)) {
            val outputFile = extractDir.resolve(sanitizeFileName(outputFileName))
            createDirectories(outputFile.parent)

            try {
                streamingCopyWithLimit(bzip2Stream, outputFile, maxFileSizeBytes)
                extractedFiles.add(outputFile)
                logger.info("Extracted: $outputFileName")
            } catch (e: Exception) {
                logger.severe("Failed to extract $outputFileName: ${e.message}")
                outputFile.deleteIfExists()
                throw e
            }
        }
        bzip2Stream.close()
    }

    return extractedFiles
}

private fun extractGeneric(
    archiveFile: Path,
    extractDir: Path,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<Path> {
    val extractedFiles = mutableListOf<Path>()
    try {
        archiveFile.inputStream().use { fis ->
            val factory = ArchiveStreamFactory()
            val archiveStream = factory.createArchiveInputStream(fis) as ArchiveInputStream<ArchiveEntry>
            var entry: ArchiveEntry? = archiveStream.nextEntry
            while (entry != null) {
                if (!entry.isDirectory && isXmlFile(entry.name, xmlExtensions)) {
                    if (entry.size > maxFileSizeBytes) {
                        logger.warning("Skipping file ${entry.name}: size ${entry.size} exceeds limit $maxFileSizeBytes")
                    } else {
                        val outputFile = extractDir.resolve(sanitizeFileName(entry.name))
                        createDirectories(outputFile.parent)
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
            archiveStream.close()
        }
        return extractedFiles
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
