package ru.my

import org.slf4j.LoggerFactory
import java.io.BufferedInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.*
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants.*
import javax.xml.stream.XMLStreamReader
import kotlin.io.path.exists
import kotlin.io.path.inputStream
import kotlin.io.path.name
import kotlin.system.measureTimeMillis
import kotlin.collections.ArrayDeque as KotlinArrayDeque

private val logger = LoggerFactory.getLogger("XmlParser")

enum class XmlValueType { ATTRIBUTE, CONTENT }

data class XmlValueConfig(
    val path: List<String>,
    val valueType: XmlValueType,
    val required: Boolean = false,
    val outputKey: String = path.last(),
) {
    init {
        if (path.isEmpty()) {
            throw IllegalArgumentException("Path must not be empty")
        }

        if (outputKey.isEmpty()) {
            throw IllegalArgumentException("Output key must not be empty (path: $path)")
        }
    }
}

fun parseXmlElements(
    file: Path,
    rootPath: List<String>,
    valueConfigs: Set<XmlValueConfig>,
    enumValues: Map<String, Set<String>> = emptyMap(),
    encoding: Charset? = null,
): Sequence<Map<String, String>> = sequence {
    if (rootPath.isEmpty()) {
        throw IllegalArgumentException("Root path must not be empty")
    }

    if (valueConfigs.isEmpty()) {
        throw IllegalArgumentException("Value configs must not be empty")
    }

    if (!file.exists()) {
        logger.warn("File ${file.toAbsolutePath()} not found")
        return@sequence
    }

    // Валидация на дублирующиеся пути
    val duplicatePaths = valueConfigs
        .groupBy { it.path.joinToString("/") }
        .filter { it.value.size > 1 }
        .keys

    if (duplicatePaths.isNotEmpty()) {
        throw IllegalArgumentException("Duplicate paths found in valueConfigs: $duplicatePaths")
    }

    val encodingInfo = if (encoding != null) {
        EncodingInfo(encoding)
    } else {
        val detectedEncoding = detectXmlEncoding(file)
        logger.info("Detected encoding for ${file.toAbsolutePath()}: $detectedEncoding")
        detectedEncoding
    }

    yieldAll(parseXmlElementsWithEncoding(
        file = file,
        rootPath = rootPath,
        valueConfigs = valueConfigs,
        enumValues = enumValues,
        encodingInfo = encodingInfo,
    ))
}

private fun parseXmlElementsWithEncoding(
    file: Path,
    rootPath: List<String>,
    valueConfigs: Iterable<XmlValueConfig>,
    enumValues: Map<String, Set<String>>,
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
    var skippedCount = 0
    var xmlReader: XMLStreamReader? = null
    val currentPath = mutableListOf<String>()
    val elementStack = KotlinArrayDeque<XmlNode>()

    val parsingTime = measureTimeMillis {
        try {
            file.inputStream().use { inputStream ->
                // Пропускаем BOM если он есть
                if (encodingInfo.bomSize > 0) {
                    inputStream.skip(encodingInfo.bomSize.toLong())
                }

                val bufferedStream = BufferedInputStream(inputStream)
                val reader = InputStreamReader(bufferedStream, encodingInfo.charset)
                val bufferedReader = BufferedReader(reader, 8192)

                val streamReader = xmlInputFactory.createXMLStreamReader(bufferedReader)
                xmlReader = streamReader // сохраняем для finally блока

                while (streamReader.hasNext()) {
                    val eventType = streamReader.next()

                    when (eventType) {
                        START_ELEMENT -> {
                            val tagName = streamReader.localName
                            currentPath.add(tagName)

                            // Получаем атрибуты текущего элемента
                            val attributes = (0 until streamReader.attributeCount).associate { i ->
                                val key = streamReader.getAttributeLocalName(i)
                                val value = streamReader.getAttributeValue(i)
                                key to (value?.trim() ?: "")
                            }

                            // Создаем новый элемент
                            val newElement = XmlNode(
                                tagName = tagName,
                                attributes = attributes,
                                content = StringBuilder()
                            )

                            // Добавляем как дочерний элемент к последнему в стеке
                            if (elementStack.isNotEmpty()) {
                                elementStack.last().children.add(newElement)
                            }

                            elementStack.add(newElement)

                            // Проверяем, достигли ли мы корневого элемента для обработки
                            val isRootElement = rootPath.isNotEmpty() &&
                                    currentPath.size >= rootPath.size &&
                                    currentPath.takeLast(rootPath.size) == rootPath

                            if (isRootElement) {
                                newElement.isRoot = true
                            }
                        }

                        CHARACTERS -> {
                            if (elementStack.isNotEmpty()) {
                                val text = streamReader.text ?: ""
                                elementStack.last().content.append(text)
                            }
                        }

                        END_ELEMENT -> {
                            if (elementStack.isNotEmpty()) {
                                val closedElement = elementStack.removeLast()
                                val closedPath = currentPath.joinToString("/") // Сохраняем путь для логирования

                                // Если это корневой элемент, собираем все данные
                                if (closedElement.isRoot) {
                                    val result = collectElementData(closedElement, valueConfigs)

                                    if (result.isNotEmpty()) {
                                        // Проверяем enum ограничения
                                        if (isValidRecord(result, valueConfigs, enumValues)) {
                                            yield(result)
                                            processedCount++

                                            if (processedCount % 10000 == 0) {
                                                logger.info(buildString {
                                                    append("Processed $processedCount records ")
                                                    append("(path: $closedPath)")
                                                    append(" from ${file.toAbsolutePath()}")
                                                })
                                            }
                                        } else {
                                            skippedCount++
                                        }
                                    }
                                }
                            }

                            if (currentPath.isNotEmpty()) {
                                currentPath.removeLast()
                            }
                        }
                    }
                }
            }

        } catch (e: Exception) {
            logger.error("Error parsing file ${file.toAbsolutePath()}: ${e.message}")
            throw e
        } finally {
            try {
                xmlReader?.close()
            } catch (e: Exception) {
                logger.warn("Error closing XML stream: ${e.message}")
            }
        }
    }

    logger.info(buildString {
        append("Completed parsing ${file.toAbsolutePath()}: ")
        append("$processedCount records processed, ")
        append("$skippedCount skipped, ")
        append("took ${parsingTime}ms")
    })
}

private data class XmlNode(
    val tagName: String,
    val attributes: Map<String, String>,
    val content: StringBuilder,
    var isRoot: Boolean = false,
    val children: MutableList<XmlNode> = mutableListOf()
)

private fun collectElementData(
    rootElement: XmlNode,
    valueConfigs: Iterable<XmlValueConfig>
): Map<String, String> {
    val result = mutableMapOf<String, String>()

    // Создаем индекс всех элементов по путям — начиная с корневого тега
    val elementsByPath = mutableMapOf<List<String>, XmlNode>()
    buildElementIndex(rootElement, listOf(rootElement.tagName), elementsByPath)

    // Обрабатываем каждую конфигурацию значений
    for (config in valueConfigs) {
        val value = when (config.valueType) {
            XmlValueType.ATTRIBUTE -> {
                // Для атрибутов: путь до элемента + имя атрибута
                val elementPath = config.path.dropLast(1)
                val attributeName = config.path.last()
                val fullPath = listOf(rootElement.tagName) + elementPath
                elementsByPath[fullPath]?.attributes?.get(attributeName)
            }
            XmlValueType.CONTENT -> {
                // Для контента: полный путь = корневой тег + путь из конфига
                val fullPath = listOf(rootElement.tagName) + config.path
                elementsByPath[fullPath]?.content?.toString()?.trim()?.takeIf { it.isNotEmpty() }
            }
        }

        if (value != null) {
            val key = config.outputKey
            if (key in result) {
                logger.warn(buildString {
                    append("Output key '$key' already exists in result ")
                    append("(path: ${config.path.joinToString("/")}). ")
                    append("Overwriting value.")
                })
            }
            result[key] = value
        }
    }

    return result
}

private fun buildElementIndex(
    element: XmlNode,
    currentPath: List<String>,
    index: MutableMap<List<String>, XmlNode>
) {
    // Добавляем текущий элемент в индекс
    index[currentPath] = element

    // Рекурсивно обрабатываем дочерние элементы
    for (child in element.children) {
        val childPath = currentPath + child.tagName
        buildElementIndex(child, childPath, index)
    }
}

private fun isValidRecord(
    result: Map<String, String>,
    valueConfigs: Iterable<XmlValueConfig>,
    enumValues: Map<String, Set<String>> = emptyMap(),
): Boolean {
    return valueConfigs.all { config ->
        val value = result[config.outputKey]

        if (config.required && value.isNullOrBlank()) {
            logger.debug("Record invalid: required field missing at ${config.outputKey}")
            return@all false
        }

        val enumSet = enumValues[config.outputKey]
        val isValid = enumSet.isNullOrEmpty() || enumSet.contains(value)

        if (!isValid) {
            logger.debug("Record invalid: value '$value' not in enum for ${config.outputKey}")
        }

        isValid
    }
}

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

fun isXmlFile(fileName: String, extensions: Set<String> = setOf("xml")): Boolean {
    val ext = fileName.substringAfterLast('.', "").lowercase()
    return extensions.any { it.equals(ext, ignoreCase = true) }
}
