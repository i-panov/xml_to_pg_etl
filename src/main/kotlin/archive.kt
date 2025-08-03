package ru.my

import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.ArchiveInputStream
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.compressors.CompressorStreamFactory
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.util.logging.Logger

private val logger = Logger.getLogger("ArchiveExtractor")

// Размер буфера для streaming копирования (8KB по умолчанию)
private const val BUFFER_SIZE = 8192
// Максимальный размер файла для извлечения (например, 1GB)
private const val MAX_FILE_SIZE = 1024L * 1024L * 1024L

/**
 * Универсальная функция для извлечения архивов различных форматов с streaming обработкой
 * Поддерживает: ZIP, TAR, TAR.GZ, TAR.BZ2, 7Z, GZIP, BZIP2 и др.
 *
 * @param archivePath путь к архиву
 * @param extractTo папка для извлечения
 * @param xmlFileExtensions расширения XML файлов для извлечения
 * @param removeArchiveAfterExtraction удалять ли архив после успешного извлечения
 * @param maxFileSizeBytes максимальный размер извлекаемого файла в байтах
 * @return список путей к извлеченным XML файлам
 */
fun extractArchive(
    archivePath: String,
    extractTo: String,
    xmlFileExtensions: Set<String> = setOf("xml"),
    removeArchiveAfterExtraction: Boolean = false,
    maxFileSizeBytes: Long = MAX_FILE_SIZE
): List<String> {
    val archiveFile = File(archivePath)
    if (!archiveFile.exists()) {
        throw IllegalArgumentException("Archive file not found: $archivePath")
    }

    val extractDir = File(extractTo)
    extractDir.mkdirs()

    logger.info("Extracting archive: ${archiveFile.name} to $extractTo")

    val extractedFiles = when {
        archivePath.endsWith(".zip", ignoreCase = true) ->
            extractWithFormat(archiveFile, extractDir, ArchiveStreamFactory.ZIP, xmlFileExtensions, maxFileSizeBytes)
        archivePath.endsWith(".tar.gz", ignoreCase = true) ||
                archivePath.endsWith(".tgz", ignoreCase = true) ->
            extractTarGz(archiveFile, extractDir, xmlFileExtensions, maxFileSizeBytes)
        archivePath.endsWith(".tar.bz2", ignoreCase = true) ||
                archivePath.endsWith(".tbz2", ignoreCase = true) ->
            extractTarBz2(archiveFile, extractDir, xmlFileExtensions, maxFileSizeBytes)
        archivePath.endsWith(".tar", ignoreCase = true) ->
            extractWithFormat(archiveFile, extractDir, ArchiveStreamFactory.TAR, xmlFileExtensions, maxFileSizeBytes)
        archivePath.endsWith(".gz", ignoreCase = true) ->
            extractGzip(archiveFile, extractDir, xmlFileExtensions, maxFileSizeBytes)
        archivePath.endsWith(".bz2", ignoreCase = true) ->
            extractBzip2(archiveFile, extractDir, xmlFileExtensions, maxFileSizeBytes)
        archivePath.endsWith(".7z", ignoreCase = true) ->
            extractWithFormat(archiveFile, extractDir, ArchiveStreamFactory.SEVEN_Z, xmlFileExtensions, maxFileSizeBytes)
        else -> extractGeneric(archiveFile, extractDir, xmlFileExtensions, maxFileSizeBytes)
    }

    // Удаляем архив после успешного извлечения
    if (removeArchiveAfterExtraction && extractedFiles.isNotEmpty()) {
        if (archiveFile.delete()) {
            logger.info("Deleted archive: $archivePath")
        } else {
            logger.warning("Failed to delete archive: $archivePath")
        }
    }

    return extractedFiles
}

private fun extractWithFormat(
    archiveFile: File,
    extractDir: File,
    format: String,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<String> {
    val extractedFiles = mutableListOf<String>()

    FileInputStream(archiveFile).use { fis ->
        val factory = ArchiveStreamFactory()
        val archiveStream: ArchiveInputStream<out ArchiveEntry> = factory.createArchiveInputStream(format, fis)

        var entry: ArchiveEntry? = archiveStream.nextEntry
        while (entry != null) {
            if (!entry.isDirectory && isXmlFile(entry.name, xmlExtensions)) {
                // Проверяем размер файла перед извлечением
                if (entry.size > maxFileSizeBytes) {
                    logger.warning("Skipping file ${entry.name}: size ${entry.size} exceeds limit $maxFileSizeBytes")
                } else {
                    val outputFile = File(extractDir, sanitizeFileName(entry.name))
                    outputFile.parentFile?.mkdirs()

                    try {
                        // Streaming копирование с буфером
                        streamingCopy(archiveStream, outputFile, entry.size)
                        extractedFiles.add(outputFile.absolutePath)
                        logger.info("Extracted: ${entry.name} (${entry.size} bytes)")
                    } catch (e: Exception) {
                        logger.severe("Failed to extract ${entry.name}: ${e.message}")
                        // Удаляем частично созданный файл
                        outputFile.delete()
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
    archiveFile: File,
    extractDir: File,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<String> {
    val extractedFiles = mutableListOf<String>()

    FileInputStream(archiveFile).use { fis ->
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
                    val outputFile = File(extractDir, sanitizeFileName(entry.name))
                    outputFile.parentFile?.mkdirs()

                    try {
                        streamingCopy(tarStream, outputFile, entry.size)
                        extractedFiles.add(outputFile.absolutePath)
                        logger.info("Extracted: ${entry.name} (${entry.size} bytes)")
                    } catch (e: Exception) {
                        logger.severe("Failed to extract ${entry.name}: ${e.message}")
                        outputFile.delete()
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
    archiveFile: File,
    extractDir: File,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<String> {
    val extractedFiles = mutableListOf<String>()

    FileInputStream(archiveFile).use { fis ->
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
                    val outputFile = File(extractDir, sanitizeFileName(entry.name))
                    outputFile.parentFile?.mkdirs()

                    try {
                        streamingCopy(tarStream, outputFile, entry.size)
                        extractedFiles.add(outputFile.absolutePath)
                        logger.info("Extracted: ${entry.name} (${entry.size} bytes)")
                    } catch (e: Exception) {
                        logger.severe("Failed to extract ${entry.name}: ${e.message}")
                        outputFile.delete()
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
    archiveFile: File,
    extractDir: File,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<String> {
    val extractedFiles = mutableListOf<String>()

    FileInputStream(archiveFile).use { fis ->
        val compressorFactory = CompressorStreamFactory()
        val gzipStream = compressorFactory.createCompressorInputStream(CompressorStreamFactory.GZIP, fis)

        val outputFileName = archiveFile.nameWithoutExtension
        if (isXmlFile(outputFileName, xmlExtensions)) {
            val outputFile = File(extractDir, sanitizeFileName(outputFileName))

            try {
                // Для GZIP файлов размер неизвестен заранее, поэтому используем ограниченный поток
                streamingCopyWithLimit(gzipStream, outputFile, maxFileSizeBytes)
                extractedFiles.add(outputFile.absolutePath)
                logger.info("Extracted: $outputFileName")
            } catch (e: Exception) {
                logger.severe("Failed to extract $outputFileName: ${e.message}")
                outputFile.delete()
                throw e
            }
        }
        gzipStream.close()
    }

    return extractedFiles
}

private fun extractBzip2(
    archiveFile: File,
    extractDir: File,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<String> {
    val extractedFiles = mutableListOf<String>()

    FileInputStream(archiveFile).use { fis ->
        val compressorFactory = CompressorStreamFactory()
        val bzip2Stream = compressorFactory.createCompressorInputStream(CompressorStreamFactory.BZIP2, fis)

        val outputFileName = archiveFile.nameWithoutExtension
        if (isXmlFile(outputFileName, xmlExtensions)) {
            val outputFile = File(extractDir, sanitizeFileName(outputFileName))

            try {
                streamingCopyWithLimit(bzip2Stream, outputFile, maxFileSizeBytes)
                extractedFiles.add(outputFile.absolutePath)
                logger.info("Extracted: $outputFileName")
            } catch (e: Exception) {
                logger.severe("Failed to extract $outputFileName: ${e.message}")
                outputFile.delete()
                throw e
            }
        }
        bzip2Stream.close()
    }

    return extractedFiles
}

private fun extractGeneric(
    archiveFile: File,
    extractDir: File,
    xmlExtensions: Set<String>,
    maxFileSizeBytes: Long
): List<String> {
    logger.info("Attempting generic extraction for: ${archiveFile.name}")

    try {
        FileInputStream(archiveFile).use { fis ->
            val factory = ArchiveStreamFactory()
            val archiveStream: ArchiveInputStream<out ArchiveEntry> = factory.createArchiveInputStream(fis)

            val extractedFiles = mutableListOf<String>()
            var entry: ArchiveEntry? = archiveStream.nextEntry
            while (entry != null) {
                if (!entry.isDirectory && isXmlFile(entry.name, xmlExtensions)) {
                    if (entry.size > maxFileSizeBytes) {
                        logger.warning("Skipping file ${entry.name}: size ${entry.size} exceeds limit $maxFileSizeBytes")
                    } else {
                        val outputFile = File(extractDir, sanitizeFileName(entry.name))
                        outputFile.parentFile?.mkdirs()

                        try {
                            streamingCopy(archiveStream, outputFile, entry.size)
                            extractedFiles.add(outputFile.absolutePath)
                            logger.info("Extracted: ${entry.name} (${entry.size} bytes)")
                        } catch (e: Exception) {
                            logger.severe("Failed to extract ${entry.name}: ${e.message}")
                            outputFile.delete()
                            throw e
                        }
                    }
                }
                entry = archiveStream.nextEntry
            }
            archiveStream.close()

            return extractedFiles
        }
    } catch (e: Exception) {
        logger.severe("Failed to extract archive ${archiveFile.name}: ${e.message}")
        throw IllegalArgumentException("Unsupported archive format: ${archiveFile.name}", e)
    }
}

/**
 * Копирование с буфером для известного размера файла
 */
private fun streamingCopy(inputStream: InputStream, outputFile: File, expectedSize: Long) {
    FileOutputStream(outputFile).use { fos ->
        val buffer = ByteArray(BUFFER_SIZE)
        var totalBytes = 0L
        var bytesRead: Int

        while (inputStream.read(buffer).also { bytesRead = it } != -1) {
            fos.write(buffer, 0, bytesRead)
            totalBytes += bytesRead

            // Проверяем, что не превышаем ожидаемый размер (защита от zip bombs)
            if (totalBytes > expectedSize * 2) {
                throw IllegalStateException("File size exceeds expected size by 2x: expected $expectedSize, got $totalBytes")
            }
        }

        logger.fine("Copied $totalBytes bytes to ${outputFile.name}")
    }
}

/**
 * Копирование с ограничением размера для неизвестного размера файла
 */
private fun streamingCopyWithLimit(inputStream: InputStream, outputFile: File, maxSize: Long) {
    FileOutputStream(outputFile).use { fos ->
        val buffer = ByteArray(BUFFER_SIZE)
        var totalBytes = 0L
        var bytesRead: Int

        while (inputStream.read(buffer).also { bytesRead = it } != -1) {
            if (totalBytes + bytesRead > maxSize) {
                throw IllegalStateException("File size exceeds maximum allowed size: $maxSize")
            }

            fos.write(buffer, 0, bytesRead)
            totalBytes += bytesRead
        }

        logger.fine("Copied $totalBytes bytes to ${outputFile.name}")
    }
}

private fun isXmlFile(fileName: String, xmlExtensions: Set<String>): Boolean {
    val extension = fileName.substringAfterLast('.', "").lowercase()
    return extension in xmlExtensions.map { it.lowercase() }
}

private fun sanitizeFileName(fileName: String): String {
    return fileName
        .replace("\\", "/")
        .split("/")
        .last()
        .replace(Regex("[^a-zA-Z0-9._-]"), "_")
}
