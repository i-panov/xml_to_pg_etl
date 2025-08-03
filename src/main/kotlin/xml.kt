package ru.my

import java.io.*
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.util.logging.Logger
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants.*
import javax.xml.stream.XMLStreamReader

private val logger = Logger.getLogger("XmlParser")

/**
 * Парсит XML элементы с автоматическим определением кодировки
 */
fun parseXmlElements(file: File, tag: String): Sequence<Map<String, String>> = sequence {
    if (!file.exists()) {
        logger.warning("File ${file.name} not found")
        return@sequence
    }

    val encoding = detectXmlEncoding(file)
    logger.info("Detected encoding for ${file.name}: $encoding")

    // Используем перегрузку с явным указанием кодировки
    yieldAll(parseXmlElementsWithEncoding(file, tag, encoding))
}

/**
 * Парсит XML элементы с принудительным указанием кодировки
 */
fun parseXmlElements(file: File, tag: String, encoding: Charset): Sequence<Map<String, String>> = sequence {
    if (!file.exists()) {
        logger.warning("File ${file.name} not found")
        return@sequence
    }

    yieldAll(parseXmlElementsWithEncoding(file, tag, encoding))
}

private fun parseXmlElementsWithEncoding(
    file: File,
    tag: String,
    encoding: Charset
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

    try {
        FileInputStream(file).use { inputStream ->
            // Буферизация для улучшения производительности
            val bufferedStream = BufferedInputStream(inputStream)
            val reader = InputStreamReader(bufferedStream, encoding)
            val bufferedReader = BufferedReader(reader, 8192)

            xmlReader = xmlInputFactory.createXMLStreamReader(bufferedReader)
            while (xmlReader!!.hasNext()) {
                val eventType = xmlReader!!.next()

                if (eventType == START_ELEMENT && xmlReader!!.localName == tag) {
                    val attributes = (0 until xmlReader!!.attributeCount).associate { i ->
                        xmlReader!!.getAttributeLocalName(i) to (xmlReader!!.getAttributeValue(i)?.trim() ?: "")
                    }

                    if (attributes.isNotEmpty()) {
                        yield(attributes)
                        processedCount++

                        if (processedCount % 10000 == 0) {
                            logger.info("Processed $processedCount records from ${file.name}")
                        }
                    }

                    // Пропускаем содержимое элемента до закрывающего тега
                    if (xmlReader!!.next() != END_ELEMENT) {
                        var depth = 1
                        while (xmlReader!!.hasNext() && depth > 0) {
                            when (xmlReader!!.next()) {
                                START_ELEMENT -> depth++
                                END_ELEMENT -> depth--
                            }
                            if (depth == 0 && xmlReader!!.localName == tag) break
                        }
                    }
                }
            }
        }

        logger.info("Completed parsing ${file.name}: $processedCount records")

    } catch (e: Exception) {
        logger.severe("Error parsing file ${file.name}: ${e.message}")
        throw e
    } finally {
        try {
            xmlReader?.close()
        } catch (e: Exception) {
            logger.warning("Error closing XML stream: ${e.message}")
        }
    }
}

/**
 * Определяет кодировку XML файла из XML declaration или BOM
 */
fun detectXmlEncoding(file: File): Charset {
    try {
        FileInputStream(file).use { inputStream ->
            // Читаем только первые 4 байта для BOM
            val bomBuffer = ByteArray(4)
            val bytesRead = inputStream.read(bomBuffer)

            if (bytesRead <= 0) {
                logger.warning("Empty file ${file.name}, using UTF-8")
                return StandardCharsets.UTF_8
            }

            // Проверяем BOM на первых считанных байтах
            detectBOM(bomBuffer, bytesRead)?.let {
                logger.fine("Detected BOM encoding: ${it.name()}")
                return it
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
            return parseXmlDeclarationEncoding(headerText) ?: StandardCharsets.UTF_8
        }
    } catch (e: Exception) {
        logger.warning("Failed to detect encoding for ${file.name}: ${e.message}, using UTF-8")
        return StandardCharsets.UTF_8
    }
}

/**
 * Определяет кодировку по BOM (Byte Order Mark)
 */
private fun detectBOM(buffer: ByteArray, length: Int): Charset? {
    // UTF-8 BOM: EF BB BF
    if (length >= 3 &&
        buffer[0] == 0xEF.toByte() &&
        buffer[1] == 0xBB.toByte() &&
        buffer[2] == 0xBF.toByte()) {
        return StandardCharsets.UTF_8
    }

    // UTF-16 BE BOM: FE FF
    if (length >= 2 &&
        buffer[0] == 0xFE.toByte() &&
        buffer[1] == 0xFF.toByte()) {
        return StandardCharsets.UTF_16BE
    }

    // UTF-16 LE BOM: FF FE
    if (length >= 2 &&
        buffer[0] == 0xFF.toByte() &&
        buffer[1] == 0xFE.toByte()) {
        return StandardCharsets.UTF_16LE
    }

    // UTF-32 BE BOM: 00 00 FE FF
    if (length >= 4 &&
        buffer[0] == 0x00.toByte() &&
        buffer[1] == 0x00.toByte() &&
        buffer[2] == 0xFE.toByte() &&
        buffer[3] == 0xFF.toByte()) {
        return Charset.forName("UTF-32BE")
    }

    // UTF-32 LE BOM: FF FE 00 00
    if (length >= 4 &&
        buffer[0] == 0xFF.toByte() &&
        buffer[1] == 0xFE.toByte() &&
        buffer[2] == 0x00.toByte() &&
        buffer[3] == 0x00.toByte()) {
        return Charset.forName("UTF-32LE")
    }

    return null
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
        logger.fine("Found encoding declaration: $encodingName")

        return try {
            Charset.forName(encodingName.trim())
        } catch (e: Exception) {
            logger.warning("Unsupported encoding '$encodingName' in XML declaration")
            null
        }
    } catch (e: Exception) {
        logger.fine("Error parsing XML declaration: ${e.message}")
        return null
    }
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
