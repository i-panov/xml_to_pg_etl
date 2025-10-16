package ru.my.xml

import org.slf4j.LoggerFactory
import java.nio.charset.Charset
import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.system.measureTimeMillis

private val logger = LoggerFactory.getLogger("XmlParser")

private const val LOG_INTERVAL = 50_000

enum class XmlValueType { ATTRIBUTE, CONTENT }

data class XmlValueConfig(
    val path: List<String>,
    val valueType: XmlValueType,
    val required: Boolean = false,
    val notForSave: Boolean = false,
    val outputKey: String,
) {
    init {
        require(outputKey.isNotEmpty()) {
            "Output key must not be empty (path: ${path.joinToString("/")})"
        }

        if (valueType == XmlValueType.ATTRIBUTE) {
            require(path.isNotEmpty()) {
                "Attribute path must contain at least attribute name for outputKey: $outputKey"
            }
        }
    }
}

/**
 * Парсит XML-файл потоковым образом, извлекая данные согласно заданным конфигурациям.
 *
 * Использует функцию toNodes() для преобразования событий в структурированные узлы,
 * что значительно упрощает логику извлечения данных.
 *
 * ВАЖНО:
 * - Возвращаемая Sequence не является thread-safe и должна обрабатываться последовательно.
 * - При совпадении нескольких элементов на одном пути берется последнее значение.
 * - Пустые текстовые узлы игнорируются (trim() применяется автоматически).
 * - Каждый элемент полностью загружается в память, но обработка остается потоковой.
 *
 * @param file Путь к XML-файлу.
 * @param rootPath Абсолютный путь от корня XML к элементу, который считается "корневым" для извлечения одной записи.
 *                 Пример: listOf("catalog", "book") для пути /catalog/book
 * @param valueConfigs Набор конфигураций, определяющих, какие значения (атрибуты или контент)
 *                     и по каким путям следует извлекать. Пути в XmlValueConfig задаются относительно rootPath.
 * @param enumValues Карта для валидации значений по перечислениям. Ключ - outputKey из XmlValueConfig,
 *                   значение - набор допустимых значений. Пустое множество означает отсутствие валидации.
 * @param encoding Явная кодировка файла. Если null, кодировка будет детектирована автоматически из BOM или XML declaration.
 * @return Последовательность карт, где каждая карта представляет собой одну извлеченную запись.
 *         Ключи в карте соответствуют outputKey из XmlValueConfig (за исключением notForSave полей).
 * @throws XmlParsingException при ошибках парсинга XML
 * @throws IllegalArgumentException при некорректной конфигурации
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

    // Проверка на дублирующиеся outputKey
    val duplicateOutputKeys = valueConfigs
        .groupBy { it.outputKey }
        .filter { it.value.size > 1 }
        .keys

    require(duplicateOutputKeys.isEmpty()) {
        "Duplicate output keys found in valueConfigs: ${duplicateOutputKeys.joinToString()}"
    }

    val encodingInfo = encoding?.let { EncodingInfo(it) } ?: detectXmlEncoding(file).also {
        logger.info("Detected encoding for ${file.toAbsolutePath()}: ${it.charset}")
    }

    val validationOnlyKeys = valueConfigs
        .filter { it.notForSave }
        .mapTo(mutableSetOf()) { it.outputKey }

    return sequence {
        var processedCount = 0
        var skippedCount = 0

        val parsingTime = measureTimeMillis {
            try {
                val nodes = file.iterateXml(encodingInfo.charset, encodingInfo.bomSize)
                    .asIterable().toNodes(rootPath)

                for (node in nodes) {
                    // Извлекаем значения из узла
                    val recordData = extractValuesFromNode(node, valueConfigs)

                    // Валидация
                    if (isValidRecord(recordData, valueConfigs, enumValues)) {
                        // Фильтруем поля, помеченные как notForSave
                        val resultData = if (validationOnlyKeys.isEmpty()) {
                            recordData
                        } else {
                            recordData.filterKeys { key -> key !in validationOnlyKeys }
                        }

                        // Проверяем, что после фильтрации остались данные
                        if (resultData.isNotEmpty()) {
                            yield(resultData)
                            processedCount++

                            if (processedCount % LOG_INTERVAL == 0) {
                                logger.info(
                                    "Processed $processedCount records (path: ${rootPath.joinToString("/")}) " +
                                            "from ${file.toAbsolutePath()}"
                                )
                            }
                        } else {
                            logger.debug("Record skipped: all fields are notForSave at path ${rootPath.joinToString("/")}")
                            skippedCount++
                        }
                    } else {
                        skippedCount++
                    }
                }
            } catch (e: XmlParsingException) {
                logger.error("Error parsing file ${file.toAbsolutePath()}: ${e.message}", e)
                throw e
            } catch (e: Exception) {
                val message = "Error parsing file ${file.toAbsolutePath()} at record ${processedCount + 1}: ${e.message}"
                logger.error(message, e)
                throw XmlParsingException(message, e)
            }
        }

        logger.info(
            "Completed parsing ${file.toAbsolutePath()}: " +
                    "$processedCount records processed, " +
                    "$skippedCount skipped, " +
                    "took ${parsingTime}ms"
        )
    }
}

/**
 * Извлекает значения из узла согласно конфигурациям.
 * Это значительно проще, чем обработка событий!
 *
 * @param node Узел для извлечения данных
 * @param valueConfigs Конфигурации извлечения
 * @return Карта извлеченных значений
 */
private fun extractValuesFromNode(
    node: XmlNode,
    valueConfigs: Set<XmlValueConfig>
): Map<String, String> {
    val result = mutableMapOf<String, String>()

    for (config in valueConfigs) {
        val value = when (config.valueType) {
            XmlValueType.ATTRIBUTE -> extractAttribute(node, config.path)
            XmlValueType.CONTENT -> extractContent(node, config.path)
        }

        if (value != null && value.isNotBlank()) {
            result[config.outputKey] = value
        }
    }

    return result
}

/**
 * Извлекает атрибут из узла по пути.
 *
 * @param node Корневой узел (элемент rootPath)
 * @param path Относительный путь к атрибуту.
 *             Если содержит один элемент - это атрибут корневого узла.
 *             Если больше - последний элемент это имя атрибута, остальные - путь к элементу.
 * @return Значение атрибута или null
 */
private fun extractAttribute(node: XmlNode, path: List<String>): String? {
    return when {
        path.isEmpty() -> null
        path.size == 1 -> {
            // Атрибут корневого элемента
            node.attributes[path[0]]
        }
        else -> {
            // Атрибут вложенного элемента
            // Путь до элемента: path.dropLast(1), имя атрибута: path.last()
            val targetNode = findNodeByPath(node, path.dropLast(1))
            targetNode?.attributes?.get(path.last())
        }
    }
}

/**
 * Извлекает текстовый контент из узла по пути.
 *
 * @param node Корневой узел (элемент rootPath)
 * @param path Относительный путь к элементу.
 *             Пустой путь означает контент самого корневого узла.
 * @return Текстовый контент или null
 */
private fun extractContent(node: XmlNode, path: List<String>): String? {
    return when {
        path.isEmpty() -> {
            // Контент корневого элемента
            node.content.takeIf { it.isNotBlank() }
        }
        else -> {
            // Контент вложенного элемента
            val targetNode = findNodeByPath(node, path)
            targetNode?.content?.takeIf { it.isNotBlank() }
        }
    }
}

/**
 * Находит узел по относительному пути от корневого узла.
 *
 * @param root Корневой узел
 * @param path Относительный путь (список имен элементов)
 * @return Найденный узел или null, если путь не найден
 */
private fun findNodeByPath(root: XmlNode, path: List<String>): XmlNode? {
    if (path.isEmpty()) return root

    var current = root
    for (elementName in path) {
        // Ищем первый дочерний элемент с нужным именем
        current = current.children.find { it.name == elementName } ?: return null
    }

    return current
}

/**
 * Проверяет валидность записи согласно конфигурации.
 *
 * @param recordData Извлеченные данные
 * @param valueConfigs Конфигурации полей
 * @param enumValues Допустимые значения для enum-полей
 * @return true, если запись валидна
 */
private fun isValidRecord(
    recordData: Map<String, String>,
    valueConfigs: Set<XmlValueConfig>,
    enumValues: Map<String, Set<String>>
): Boolean {
    return valueConfigs.all { config ->
        val value = recordData[config.outputKey]

        // Проверка обязательных полей
        if (config.required && value.isNullOrBlank()) {
            logger.debug("Record invalid: required field '${config.outputKey}' missing or blank")
            return false
        }

        // Проверка enum значений (только если значение присутствует)
        if (value != null) {
            val enumSet = enumValues[config.outputKey]
            if (!enumSet.isNullOrEmpty() && value !in enumSet) {
                logger.debug(
                    "Record invalid: value '$value' not in enum for '${config.outputKey}'. " +
                            "Expected one of: ${enumSet.take(5).joinToString()}${if (enumSet.size > 5) "..." else ""}"
                )
                return false
            }
        }

        true
    }
}
