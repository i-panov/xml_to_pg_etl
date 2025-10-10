package ru.my

import org.slf4j.LoggerFactory
import java.nio.charset.Charset
import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.system.measureTimeMillis
import kotlin.collections.ArrayDeque as KotlinArrayDeque

private val logger = LoggerFactory.getLogger("XmlParser")

enum class XmlValueType { ATTRIBUTE, CONTENT }

data class XmlValueConfig(
    val path: List<String>,
    val valueType: XmlValueType,
    val required: Boolean = false,
    val notForSave: Boolean = false,
    val outputKey: String,
) {
    init {
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
 *                     и по каким путям следует извлекать. Пути в XmlValueConfig относительны rootPath.
 * @param enumValues Карта для валидации значений по перечислениям. Ключ - outputKey из XmlValueConfig,
 *                   значение - набор допустимых значений.
 * @param encoding Явная кодировка файла. Если null, кодировка будет детектирована автоматически.
 * @return Последовательность карт, где каждая карта представляет собой одну извлеченную запись.
 *         Ключи в карте соответствуют outputKey из XmlValueConfig.
 */
fun parseXmlElements(
    file: Path,
    rootPath: List<String>, // Абсолютный путь от корня XML
    valueConfigs: Set<XmlValueConfig>,
    enumValues: Map<String, Set<String>> = emptyMap(),
    encoding: Charset? = null,
): Sequence<Map<String, String>> {
    require(rootPath.isNotEmpty()) { "Root path must not be empty" }
    require(valueConfigs.isNotEmpty()) { "Value configs must not be empty" }
    require(file.exists()) { "File not found: ${file.toAbsolutePath()}" }

    // Проверка на дублирующиеся outputKey
    val duplicateOutputKeys = valueConfigs
        .groupBy { it.outputKey }
        .filter { it.value.size > 1 }
        .keys

    require(duplicateOutputKeys.isEmpty()) { "Duplicate output keys found in valueConfigs: $duplicateOutputKeys" }

    val encodingInfo = encoding?.let { EncodingInfo(it) } ?: detectXmlEncoding(file).also {
        logger.info("Detected encoding for ${file.toAbsolutePath()}: ${it.charset}")
    }

    // Предварительная обработка valueConfigs для быстрого доступа во время парсинга.
    // Группировка конфигов по их относительному пути (относительно rootPath).
    // Ключи здесь - это config.path или config.path.dropLast(1)
    val configsByRelativePath = mutableMapOf<List<String>, MutableList<XmlValueConfig>>()
    valueConfigs.forEach { config ->
        val targetRelativePath = when (config.valueType) {
            XmlValueType.ATTRIBUTE -> config.path.dropLast(1) // Путь к элементу, содержащему атрибут
            XmlValueType.CONTENT -> config.path // Путь к элементу, контент которого извлекаем
        }
        configsByRelativePath.getOrPut(targetRelativePath) { mutableListOf() }.add(config)
    }

    val notForSaveKeys = valueConfigs.filter { it.notForSave }.map { it.outputKey }.toSet()

    return sequence {
        var processedCount = 0
        var skippedCount = 0
        val currentPath = mutableListOf<String>() // Отслеживает текущий абсолютный путь элемента от корня XML
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
                            // currentPath.takeLast(rootPath.size) == rootPath - это корректная проверка
                            // на соответствие абсолютного пути rootPath.
                            if (!inRootElementScope && currentPath == rootPath) {
                                inRootElementScope = true
                                currentRecordData = mutableMapOf() // Начинаем собирать данные для новой записи
                            }

                            // Если мы внутри rootPath, извлекаем атрибуты для текущего элемента
                            if (inRootElementScope) {
                                // Получаем относительный путь текущего элемента от rootPath
                                // Например, если currentPath = ["doc", "data", "item", "subitem"]
                                // и rootPath = ["doc", "data", "item"]
                                // то relativeCurrentPath = ["subitem"]
                                val relativeCurrentPath = currentPath.drop(rootPath.size)
                                configsByRelativePath[relativeCurrentPath]?.forEach { config ->
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
                                    // Получаем относительный путь закрываемого элемента от rootPath
                                    // currentPath на этом этапе еще содержит закрывающийся элемент
                                    val relativeClosedPath = currentPath.drop(rootPath.size)
                                    configsByRelativePath[relativeClosedPath]?.forEach { config ->
                                        if (config.valueType == XmlValueType.CONTENT) {
                                            val content = closedContext.contentBuilder.toString().trim()
                                            if (content.isNotEmpty()) {
                                                currentRecordData?.put(config.outputKey, content)
                                            }
                                        }
                                    }
                                }

                                // Если закрываемый элемент является rootPath, завершаем текущую запись
                                // currentPath.size == rootPath.size && currentPath == rootPath
                                // Эта проверка должна быть после извлечения контента,
                                // так как контент корневого элемента обрабатывается при его закрытии.
                                if (currentPath == rootPath) { // Проверяем, что закрываемый элемент - это наш rootPath
                                    val result = currentRecordData ?: emptyMap()
                                    if (result.isNotEmpty() && isValidRecord(result, valueConfigs, enumValues)) {
                                        val clearResult = result.filter { (k, _) -> !notForSaveKeys.contains(k) }
                                        yield(clearResult) // Выдаем готовую запись
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

private fun isValidRecord(
    result: Map<String, String>,
    valueConfigs: Iterable<XmlValueConfig>,
    enumValues: Map<String, Set<String>> = emptyMap(),
): Boolean {
    return valueConfigs.all { config ->
        val value = result[config.outputKey]

        if (config.required && value.isNullOrBlank()) {
            logger.debug("Record invalid: required field '${config.outputKey}' missing or blank")
            return@all false
        }

        // Если значение есть и есть enumValues для этого ключа, проверяем его
        val enumSet = enumValues[config.outputKey]
        val isValid = enumSet.isNullOrEmpty() || (value != null && enumSet.contains(value))

        if (!isValid) {
            logger.debug("Record invalid: value '$value' not in enum for '${config.outputKey}'")
        }

        isValid
    }
}
