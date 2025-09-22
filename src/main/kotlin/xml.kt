package ru.my

import org.slf4j.LoggerFactory
import java.io.BufferedInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants.END_ELEMENT
import javax.xml.stream.XMLStreamConstants.START_ELEMENT
import javax.xml.stream.XMLStreamReader
import kotlin.io.path.exists
import kotlin.io.path.inputStream
import kotlin.io.path.name

private val logger = LoggerFactory.getLogger("XmlParser")

fun parseXmlElements(
    file: Path,
    tags: List<String>,
    enumValues: Map<String, Set<String>> = emptyMap(),
    encoding: Charset? = null,
): Sequence<Map<String, String>> = sequence {
    if (!file.exists()) {
        logger.warn("File ${file.name} not found")
        return@sequence
    }

    val encodingInfo = if (encoding != null) {
        EncodingInfo(encoding)
    } else {
        val detectedEncoding = detectXmlEncoding(file)
        logger.info("Detected encoding for ${file.name}: $detectedEncoding")
        detectedEncoding
    }

    yieldAll(parseXmlElementsWithEncoding(
        file = file,
        tags = tags,
        enumValues = enumValues,
        encodingInfo = encodingInfo,
    ))
}

private fun parseXmlElementsWithEncoding(
    file: Path,
    tags: List<String>,
    enumValues: Map<String, Set<String>> = emptyMap(),
    encodingInfo: EncodingInfo
): Sequence<Map<String, String>> = sequence {
    val xmlInputFactory = XMLInputFactory.newInstance().apply {
        setProperty(XMLInputFactory.IS_COALESCING, true)
        setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
        setProperty(XMLInputFactory.IS_VALIDATING, false)
        setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false)
        setProperty(XMLInputFactory.SUPPORT_DTD, false)
    }

    var processedCount = 0
    var xmlReader: XMLStreamReader? = null
    val currentPath = mutableListOf<String>()

    try {
        file.inputStream().use { inputStream ->
            // Пропускаем BOM если он есть
            if (encodingInfo.bomSize > 0) {
                inputStream.skip(encodingInfo.bomSize.toLong())
            }
            
            // Буферизация для улучшения производительности
            val bufferedStream = BufferedInputStream(inputStream)
            val reader = InputStreamReader(bufferedStream, encodingInfo.charset)
            val bufferedReader = BufferedReader(reader, 8192)

            xmlReader = xmlInputFactory.createXMLStreamReader(bufferedReader)
            while (xmlReader!!.hasNext()) {
                val eventType = xmlReader!!.next()

                when (eventType) {
                    START_ELEMENT -> {
                        val tagName = xmlReader!!.localName
                        currentPath.add(tagName)

                        // Проверяем только последние N элементов пути (где N = длина tags)
                        val shouldProcess = tags.isNotEmpty() &&
                                currentPath.size >= tags.size &&
                                currentPath.takeLast(tags.size) == tags

                        if (shouldProcess) {
                            val attributes = (0 until xmlReader!!.attributeCount).associate { i ->
                                xmlReader!!.getAttributeLocalName(i) to (xmlReader!!.getAttributeValue(i)?.trim() ?: "")
                            }

                            if (attributes.isNotEmpty()) {
                                val canHandle = enumValues.isEmpty() || enumValues
                                    .map { (k, _) -> k to attributes[k] }
                                    .filter { (_, v) -> v != null }
                                    .all { (k, v) -> enumValues[k]?.contains(v) == true }

                                if (canHandle) {
                                    yield(attributes)
                                    processedCount++

                                    if (processedCount % 10000 == 0) {
                                        logger.info("Processed $processedCount records (path: ${currentPath.joinToString("/")}) from ${file.name}")
                                    }
                                }
                            }
                        }
                    }
                    END_ELEMENT -> {
                        if (currentPath.isNotEmpty()) {
                            currentPath.removeAt(currentPath.size - 1)
                        }
                    }
                }
            }
        }

        logger.info("Completed parsing ${file.name}: $processedCount records")

    } catch (e: Exception) {
        logger.error("Error parsing file ${file.name}: ${e.message}")
        throw e
    } finally {
        try {
            xmlReader?.close()
        } catch (e: Exception) {
            logger.warn("Error closing XML stream: ${e.message}")
        }
    }
}

/**
 * Информация о кодировке XML файла
 */
data class EncodingInfo(
    val charset: Charset,
    val bomSize: Int = 0
)

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
                logger.info("Detected BOM encoding: ${charset.name()}")
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

private fun ByteArray.startsWith(prefix: ByteArray) = size >= prefix.size && contentEquals(prefix)

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
        } catch (e: Exception) {
            logger.warn("Unsupported encoding '$encodingName' in XML declaration")
            null
        }
    } catch (e: Exception) {
        logger.info("Error parsing XML declaration: ${e.message}")
        return null
    }
}

fun isXmlFile(fileName: String, extensions: Set<String> = setOf("xml")): Boolean {
    val ext = fileName.substringAfterLast('.', "").lowercase()
    return extensions.any { it.equals(ext, ignoreCase = true) }
}

/**
 * Удобные константы для часто используемых кодировок
 */
object XmlEncodings {
    val UTF_8: Charset = StandardCharsets.UTF_8
    val UTF_16: Charset = StandardCharsets.UTF_16
    val UTF_16BE: Charset = StandardCharsets.UTF_16BE
    val UTF_16LE: Charset = StandardCharsets.UTF_16LE
    val ISO_8859_1: Charset = StandardCharsets.ISO_8859_1
    val US_ASCII: Charset = StandardCharsets.US_ASCII

    // Популярные кодировки для русского языка
    val WINDOWS_1251: Charset = Charset.forName("windows-1251")
    val CP866: Charset = Charset.forName("cp866")
    val KOI8_R: Charset = Charset.forName("koi8-r")
    val KOI8_U: Charset = Charset.forName("koi8-u")
}
