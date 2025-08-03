package ru.my

import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.ArchiveInputStream
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.compressors.CompressorStreamFactory
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.util.logging.Logger

private val logger = Logger.getLogger("ArchiveExtractor")

/**
 * Универсальная функция для извлечения архивов различных форматов
 * Поддерживает: ZIP, TAR, TAR.GZ, TAR.BZ2, 7Z, GZIP, BZIP2 и др.
 *
 * @param archivePath путь к архиву
 * @param extractTo папка для извлечения
 * @param xmlFileExtensions расширения XML файлов для извлечения
 * @param removeArchiveAfterExtraction удалять ли архив после успешного извлечения
 * @return список путей к извлеченным XML файлам
 */
fun extractArchive(
    archivePath: String,
    extractTo: String,
    xmlFileExtensions: Set<String> = setOf("xml"),
    removeArchiveAfterExtraction: Boolean = false
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
            extractWithFormat(archiveFile, extractDir, ArchiveStreamFactory.ZIP, xmlFileExtensions)
        archivePath.endsWith(".tar.gz", ignoreCase = true) ||
                archivePath.endsWith(".tgz", ignoreCase = true) ->
            extractTarGz(archiveFile, extractDir, xmlFileExtensions)
        archivePath.endsWith(".tar.bz2", ignoreCase = true) ||
                archivePath.endsWith(".tbz2", ignoreCase = true) ->
            extractTarBz2(archiveFile, extractDir, xmlFileExtensions)
        archivePath.endsWith(".tar", ignoreCase = true) ->
            extractWithFormat(archiveFile, extractDir, ArchiveStreamFactory.TAR, xmlFileExtensions)
        archivePath.endsWith(".gz", ignoreCase = true) ->
            extractGzip(archiveFile, extractDir, xmlFileExtensions)
        archivePath.endsWith(".bz2", ignoreCase = true) ->
            extractBzip2(archiveFile, extractDir, xmlFileExtensions)
        archivePath.endsWith(".7z", ignoreCase = true) ->
            extractWithFormat(archiveFile, extractDir, ArchiveStreamFactory.SEVEN_Z, xmlFileExtensions)
        else -> extractGeneric(archiveFile, extractDir, xmlFileExtensions)
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
    xmlExtensions: Set<String>
): List<String> {
    val extractedFiles = mutableListOf<String>()

    FileInputStream(archiveFile).use { fis ->
        val factory = ArchiveStreamFactory()
        val archiveStream: ArchiveInputStream<out ArchiveEntry> = factory.createArchiveInputStream(format, fis)

        var entry: ArchiveEntry? = archiveStream.nextEntry
        while (entry != null) {
            if (!entry.isDirectory && isXmlFile(entry.name, xmlExtensions)) {
                val outputFile = File(extractDir, sanitizeFileName(entry.name))
                outputFile.parentFile?.mkdirs()

                FileOutputStream(outputFile).use { fos ->
                    archiveStream.copyTo(fos)
                }

                extractedFiles.add(outputFile.absolutePath)
                logger.info("Extracted: ${entry.name}")
            }
            entry = archiveStream.nextEntry
        }
        archiveStream.close()
    }

    return extractedFiles
}

private fun extractTarGz(archiveFile: File, extractDir: File, xmlExtensions: Set<String>): List<String> {
    val extractedFiles = mutableListOf<String>()

    FileInputStream(archiveFile).use { fis ->
        val compressorFactory = CompressorStreamFactory()
        val gzipStream = compressorFactory.createCompressorInputStream(CompressorStreamFactory.GZIP, fis)

        val archiveFactory = ArchiveStreamFactory()
        val tarStream: ArchiveInputStream<out ArchiveEntry> = archiveFactory.createArchiveInputStream(ArchiveStreamFactory.TAR, gzipStream)

        var entry: ArchiveEntry? = tarStream.nextEntry
        while (entry != null) {
            if (!entry.isDirectory && isXmlFile(entry.name, xmlExtensions)) {
                val outputFile = File(extractDir, sanitizeFileName(entry.name))
                outputFile.parentFile?.mkdirs()

                FileOutputStream(outputFile).use { fos ->
                    tarStream.copyTo(fos)
                }

                extractedFiles.add(outputFile.absolutePath)
                logger.info("Extracted: ${entry.name}")
            }
            entry = tarStream.nextEntry
        }
        tarStream.close()
        gzipStream.close()
    }

    return extractedFiles
}

private fun extractTarBz2(archiveFile: File, extractDir: File, xmlExtensions: Set<String>): List<String> {
    val extractedFiles = mutableListOf<String>()

    FileInputStream(archiveFile).use { fis ->
        val compressorFactory = CompressorStreamFactory()
        val bzip2Stream = compressorFactory.createCompressorInputStream(CompressorStreamFactory.BZIP2, fis)

        val archiveFactory = ArchiveStreamFactory()
        val tarStream: ArchiveInputStream<out ArchiveEntry> = archiveFactory.createArchiveInputStream(ArchiveStreamFactory.TAR, bzip2Stream)

        var entry: ArchiveEntry? = tarStream.nextEntry
        while (entry != null) {
            if (!entry.isDirectory && isXmlFile(entry.name, xmlExtensions)) {
                val outputFile = File(extractDir, sanitizeFileName(entry.name))
                outputFile.parentFile?.mkdirs()

                FileOutputStream(outputFile).use { fos ->
                    tarStream.copyTo(fos)
                }

                extractedFiles.add(outputFile.absolutePath)
                logger.info("Extracted: ${entry.name}")
            }
            entry = tarStream.nextEntry
        }
        tarStream.close()
        bzip2Stream.close()
    }

    return extractedFiles
}

private fun extractGzip(archiveFile: File, extractDir: File, xmlExtensions: Set<String>): List<String> {
    val extractedFiles = mutableListOf<String>()

    FileInputStream(archiveFile).use { fis ->
        val compressorFactory = CompressorStreamFactory()
        val gzipStream = compressorFactory.createCompressorInputStream(CompressorStreamFactory.GZIP, fis)

        // Определяем имя выходного файла
        val outputFileName = archiveFile.nameWithoutExtension
        if (isXmlFile(outputFileName, xmlExtensions)) {
            val outputFile = File(extractDir, sanitizeFileName(outputFileName))

            FileOutputStream(outputFile).use { fos ->
                gzipStream.copyTo(fos)
            }

            extractedFiles.add(outputFile.absolutePath)
            logger.info("Extracted: $outputFileName")
        }
        gzipStream.close()
    }

    return extractedFiles
}

private fun extractBzip2(archiveFile: File, extractDir: File, xmlExtensions: Set<String>): List<String> {
    val extractedFiles = mutableListOf<String>()

    FileInputStream(archiveFile).use { fis ->
        val compressorFactory = CompressorStreamFactory()
        val bzip2Stream = compressorFactory.createCompressorInputStream(CompressorStreamFactory.BZIP2, fis)

        val outputFileName = archiveFile.nameWithoutExtension
        if (isXmlFile(outputFileName, xmlExtensions)) {
            val outputFile = File(extractDir, sanitizeFileName(outputFileName))

            FileOutputStream(outputFile).use { fos ->
                bzip2Stream.copyTo(fos)
            }

            extractedFiles.add(outputFile.absolutePath)
            logger.info("Extracted: $outputFileName")
        }
        bzip2Stream.close()
    }

    return extractedFiles
}

private fun extractGeneric(archiveFile: File, extractDir: File, xmlExtensions: Set<String>): List<String> {
    logger.info("Attempting generic extraction for: ${archiveFile.name}")

    try {
        // Пробуем автоопределение формата
        FileInputStream(archiveFile).use { fis ->
            val factory = ArchiveStreamFactory()
            val archiveStream: ArchiveInputStream<out ArchiveEntry> = factory.createArchiveInputStream(fis)

            val extractedFiles = mutableListOf<String>()
            var entry: ArchiveEntry? = archiveStream.nextEntry
            while (entry != null) {
                if (!entry.isDirectory && isXmlFile(entry.name, xmlExtensions)) {
                    val outputFile = File(extractDir, sanitizeFileName(entry.name))
                    outputFile.parentFile?.mkdirs()

                    FileOutputStream(outputFile).use { fos ->
                        archiveStream.copyTo(fos)
                    }

                    extractedFiles.add(outputFile.absolutePath)
                    logger.info("Extracted: ${entry.name}")
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

private fun isXmlFile(fileName: String, xmlExtensions: Set<String>): Boolean {
    val extension = fileName.substringAfterLast('.', "").lowercase()
    return extension in xmlExtensions.map { it.lowercase() }
}

private fun sanitizeFileName(fileName: String): String {
    // Убираем потенциально опасные символы и пути
    return fileName
        .replace("\\", "/")
        .split("/")
        .last()
        .replace(Regex("[^a-zA-Z0-9._-]"), "_")
}
