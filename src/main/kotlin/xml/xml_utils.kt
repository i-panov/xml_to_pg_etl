package ru.my.xml

import org.slf4j.LoggerFactory
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.*
import kotlin.io.path.*

private val logger = LoggerFactory.getLogger("XmlUtils")

/**
 * Информация о кодировке XML файла
 */
data class EncodingInfo(val charset: Charset, val bomSize: Int = 0)

/**
 * Определяет кодировку XML файла из XML declaration или BOM
 */
fun detectXmlEncoding(file: Path): EncodingInfo {
    try {
        file.inputStream().use { inputStream ->
            // Читаем только первые 4 байта для BOM
            val bomBuffer = ByteArray(4)
            val bytesRead = inputStream.read(bomBuffer)

            if (bytesRead <= 0) {
                logger.warn("Empty file ${file.name}, using UTF-8")
                return EncodingInfo(StandardCharsets.UTF_8, 0)
            }

            // Проверяем BOM на первых считанных байтах
            detectBOM(bomBuffer, bytesRead)?.let { charset ->
                logger.info("Detected BOM encoding: ${charset.name()} in ${file.toAbsolutePath()}")
                // Определяем размер BOM вручную
                val bomSize = when (charset) {
                    StandardCharsets.UTF_8 -> 3
                    StandardCharsets.UTF_16BE, StandardCharsets.UTF_16LE -> 2
                    Charset.forName("UTF-32BE"), Charset.forName("UTF-32LE") -> 4
                    else -> 0
                }
                return EncodingInfo(charset, bomSize)
            }

            // Если BOM нет, проверяем XML declaration
            val remaining = (1024 - bytesRead).coerceAtLeast(0)
            val headerBuffer = if (remaining > 0) {
                // Добираем до 1024 байт или до конца файла
                bomBuffer.copyOf(bytesRead) + inputStream.readNBytes(remaining)
            } else {
                bomBuffer.copyOf(bytesRead)
            }

            // Для декларации используем ASCII-совместимую кодировку
            val headerText = String(headerBuffer, StandardCharsets.US_ASCII)
            val encoding = parseXmlDeclarationEncoding(headerText) ?: StandardCharsets.UTF_8
            return EncodingInfo(encoding, 0)
        }
    } catch (e: Exception) {
        logger.warn("Failed to detect encoding for ${file.name}: ${e.message}, using UTF-8")
        return EncodingInfo(StandardCharsets.UTF_8, 0)
    }
}

private fun ByteArray.startsWith(prefix: ByteArray) = size >= prefix.size && Arrays.equals(
    this, 0, prefix.size, prefix, 0, prefix.size)

private class BomRule(intBytes: IntArray, val charset: Charset) {
    val bytes = intBytes.map { it.toByte() }.toByteArray()
}

private val BOM_RULES = arrayOf(
    BomRule(intArrayOf(0xEF, 0xBB, 0xBF), StandardCharsets.UTF_8),
    BomRule(intArrayOf(0xFE, 0xFF), StandardCharsets.UTF_16BE),
    BomRule(intArrayOf(0xFF, 0xFE), StandardCharsets.UTF_16LE),
    BomRule(intArrayOf(0x00, 0x00, 0xFE, 0xFF), Charset.forName("UTF-32BE")),
    BomRule(intArrayOf(0xFF, 0xFE, 0x00, 0x00), Charset.forName("UTF-32LE")),
)

/**
 * Определяет кодировку по BOM (Byte Order Mark)
 */
private fun detectBOM(buffer: ByteArray, length: Int): Charset? {
    return BOM_RULES.firstOrNull { rule -> length >= rule.bytes.size && buffer.startsWith(rule.bytes) }?.charset
}

/**
 * Парсит encoding из XML declaration
 */
private fun parseXmlDeclarationEncoding(headerText: String): Charset? {
    try {
        // Ищем начало XML декларации
        val declStart = headerText.indexOf("<?xml")
        if (declStart < 0) return null

        // Ищем конец XML декларации
        val declEnd = headerText.indexOf("?>", declStart)
        if (declEnd < 0) return null

        // Извлекаем полную декларацию
        val xmlDeclaration = headerText.substring(declStart, declEnd + 2)

        // Ищем атрибут encoding
        val encodingRegex = """encoding\s*=\s*['"]([^'"]+)['"]""".toRegex(RegexOption.IGNORE_CASE)
        val matchResult = encodingRegex.find(xmlDeclaration) ?: return null

        val encodingName = matchResult.groupValues[1]
        logger.info("Found encoding declaration: $encodingName")

        return try {
            Charset.forName(encodingName.trim())
        } catch (_: Exception) {
            logger.warn("Unsupported encoding '$encodingName' in XML declaration")
            null
        }
    } catch (e: Exception) {
        logger.info("Error parsing XML declaration: ${e.message}")
        return null
    }
}

fun Path.isXml(): Boolean {
    return isRegularFile() && extension.equals("xml", ignoreCase = true) && fileSize() > 0
}
