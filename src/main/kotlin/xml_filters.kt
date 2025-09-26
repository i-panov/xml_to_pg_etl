package ru.my

import org.slf4j.LoggerFactory
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.*
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

/**
 * Парсит XML-файл потоковым образом, извлекая данные согласно заданным конфигурациям.
 * Оптимизировано для низкого потребления памяти при обработке больших XML-файлов любой структуры.
 *
 * @param file Путь к XML-файлу.
 * @param rootPath Путь к элементу, который считается "корневым" для извлечения одной записи.
 *                 Когда парсер входит в этот элемент, начинается сбор данных для одной записи.
 *                 Когда он выходит из этого элемента, запись считается завершенной и выдается.
 * @param valueConfigs Набор конфигураций, определяющих, какие значения (атрибуты или контент)
 *                     и по каким путям следует извлекать.
 * @param enumValues Карта для валидации значений по перечислениям. Ключ - outputKey из XmlValueConfig,
 *                   значение - набор допустимых значений.
 * @param encoding Явная кодировка файла. Если null, кодировка будет детектирована автоматически.
 * @return Последовательность карт, где каждая карта представляет собой одну извлеченную запись.
 *         Ключи в карте соответствуют outputKey из XmlValueConfig.
 */
fun parseXmlElements(
    file: Path,
    rootPath: List<String>,
    valueConfigs: Set<XmlValueConfig>,
    enumValues: Map<String, Set<String>> = emptyMap(),
    encoding: Charset? = null,
): Sequence<Map<String, String>> {
    require(rootPath.isNotEmpty()) { "Root path must not be empty" }
    require(valueConfigs.isNotEmpty()) { "Value configs must not be empty" }
    require(file.exists()) { "File not found: ${file.toAbsolutePath()}" }

    val duplicatePaths = valueConfigs
        .groupBy { it.path.joinToString("/") }
        .filter { it.value.size > 1 }
        .keys

    require(duplicatePaths.isEmpty()) { "Duplicate paths found in valueConfigs: $duplicatePaths" }

    val encodingInfo = encoding?.let { EncodingInfo(it) } ?: detectXmlEncoding(file).also {
        logger.info("Detected encoding for ${file.toAbsolutePath()}: ${it.charset}")
    }

    // Предварительная обработка valueConfigs для быстрого доступа во время парсинга.
    // Группировка конфигов по их полному пути (относительно корня XML).
    val configsByFullPath = mutableMapOf<List<String>, MutableList<XmlValueConfig>>()
    valueConfigs.forEach { config ->
        val targetPath = when (config.valueType) {
            XmlValueType.ATTRIBUTE -> config.path.dropLast(1) // Для атрибутов путь - это путь к родительскому элементу
            XmlValueType.CONTENT -> config.path // Для контента путь - это путь к самому элементу
        }
        configsByFullPath.getOrPut(targetPath) { mutableListOf() }.add(config)
    }

    return sequence {
        var processedCount = 0
        var skippedCount = 0
        val currentPath = mutableListOf<String>() // Отслеживает текущий путь элемента от корня XML
        val elementStack = KotlinArrayDeque<ElementContext>() // Стек для контекста текущих элементов

        var currentRecordData: MutableMap<String, String>? = null // Данные для текущей формируемой записи
        var inRootElementScope = false // Флаг, указывающий, что мы находимся внутри элемента rootPath

        val parsingTime = measureTimeMillis {
            try {
                for (event in file.iterateXml(encodingInfo.charset, encodingInfo.bomSize)) {
                    when (event) {
                        is StartElementEvent -> {
                            currentPath.add(event.name)
                            // Добавляем контекст текущего элемента в стек
                            elementStack.add(ElementContext(event.name, event.attributes, StringBuilder()))

                            // Проверяем, вошли ли мы в корневой элемент, который нас интересует
                            if (!inRootElementScope &&
                                currentPath.size >= rootPath.size &&
                                currentPath.takeLast(rootPath.size) == rootPath
                            ) {
                                inRootElementScope = true
                                currentRecordData = mutableMapOf() // Начинаем собирать данные для новой записи
                            }

                            // Если мы внутри rootPath, извлекаем атрибуты для текущего элемента
                            if (inRootElementScope) {
                                val fullCurrentPath = currentPath.toList()
                                configsByFullPath[fullCurrentPath]?.forEach { config ->
                                    if (config.valueType == XmlValueType.ATTRIBUTE) {
                                        event.attributes[config.path.last()]?.let { value ->
                                            currentRecordData?.put(config.outputKey, value)
                                        }
                                    }
                                }
                            }
                        }
                        is CharactersEvent -> {
                            // Если мы внутри rootPath, добавляем контент к текущему элементу в стеке
                            if (inRootElementScope && elementStack.isNotEmpty()) {
                                elementStack.last().contentBuilder.append(event.content)
                            }
                        }
                        is EndElementEvent -> {
                            if (elementStack.isNotEmpty()) {
                                val closedContext = elementStack.removeLast()

                                // Если мы внутри rootPath, извлекаем контент для закрываемого элемента
                                if (inRootElementScope) {
                                    val fullClosedPath = currentPath.toList()
                                    configsByFullPath[fullClosedPath]?.forEach { config ->
                                        if (config.valueType == XmlValueType.CONTENT) {
                                            val content = closedContext.contentBuilder.toString().trim()
                                            if (content.isNotEmpty()) {
                                                currentRecordData?.put(config.outputKey, content)
                                            }
                                        }
                                    }
                                }

                                // Если закрываемый элемент является rootPath, завершаем текущую запись
                                if (currentPath.size == rootPath.size &&
                                    currentPath.takeLast(rootPath.size) == rootPath
                                ) {
                                    val result = currentRecordData ?: emptyMap()
                                    if (result.isNotEmpty() && isValidRecord(result, valueConfigs, enumValues)) {
                                        yield(result) // Выдаем готовую запись
                                        processedCount++

                                        if (processedCount % 50000 == 0) {
                                            logger.info("Processed $processedCount records (path: ${rootPath.joinToString("/")}) from ${file.toAbsolutePath()}")
                                        }
                                    } else {
                                        skippedCount++
                                    }
                                    inRootElementScope = false // Вышли из области rootPath
                                    currentRecordData = null // Сбрасываем данные для следующей записи
                                }
                            }

                            // Удаляем последний элемент из текущего пути, так как элемент закрыт
                            if (currentPath.isNotEmpty()) {
                                currentPath.removeLast()
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                logger.error("Error parsing file ${file.toAbsolutePath()}: ${e.message}", e)
                throw e
            }
        }

        logger.info("Completed parsing ${file.toAbsolutePath()}: $processedCount records processed, $skippedCount skipped, took ${parsingTime}ms")
    }
}

private data class ElementContext(
    val tagName: String,
    val attributes: Map<String, String>,
    val contentBuilder: StringBuilder
)

private data class XmlNode(
    val tagName: String,
    val attributes: Map<String, String>,
    val content: StringBuilder,
    var isRoot: Boolean = false,
    val children: MutableList<XmlNode> = mutableListOf()
)

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
