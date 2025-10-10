package ru.my

import org.slf4j.LoggerFactory
import java.nio.charset.Charset
import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.system.measureTimeMillis

private val logger = LoggerFactory.getLogger("XmlParser")

private const val LOG_INTERVAL = 50_000
private const val MAX_CONTENT_SIZE = 10_485_760 // 10 MB

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
 * Парсит XML-файл потоковым образом (StAX), извлекая данные согласно заданным конфигурациям.
 * Оптимизировано для низкого потребления памяти при обработке больших XML-файлов любой структуры.
 *
 * ВАЖНО:
 * - Возвращаемая Sequence не является thread-safe и должна обрабатываться последовательно.
 * - При совпадении нескольких элементов на одном пути берется последнее значение.
 * - Пустые текстовые узлы игнорируются (trim() применяется автоматически).
 *
 * @param file Путь к XML-файлу.
 * @param rootPath Абсолютный путь от корня XML к элементу, который считается "корневым" для извлечения одной записи.
 *                 Когда парсер входит в этот элемент, начинается сбор данных для одной записи.
 *                 Когда он выходит из этого элемента, запись считается завершенной и выдается.
 *                 Пример: listOf("catalog", "book") для пути /catalog/book
 * @param valueConfigs Набор конфигураций, определяющих, какие значения (атрибуты или контент)
 *                     и по каким путям следует извлекать. Пути в XmlValueConfig задаются относительно rootPath.
 * @param enumValues Карта для валидации значений по перечислениям. Ключ - outputKey из XmlValueConfig,
 *                   значение - набор допустимых значений. Пустое множество означает отсутствие валидации.
 * @param encoding Явная кодировка файла. Если null, кодировка будет детектирована автоматически из BOM или XML declaration.
 * @return Последовательность карт, где каждая карта представляет собой одну извлеченную запись.
 *         Ключи в карте соответствуют outputKey из XmlValueConfig (за исключением validationOnly полей).
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

    // Предварительная обработка valueConfigs для быстрого доступа во время парсинга
    val configIndex = buildConfigIndex(valueConfigs)

    val validationOnlyKeys = valueConfigs
        .filter { it.notForSave }
        .mapTo(mutableSetOf()) { it.outputKey }

    return sequence {
        var processedCount = 0
        var skippedCount = 0

        val parser = XmlRecordParser(
            rootPath = rootPath,
            configIndex = configIndex,
            valueConfigs = valueConfigs,
            enumValues = enumValues,
            validationOnlyKeys = validationOnlyKeys
        )

        val parsingTime = measureTimeMillis {
            try {
                for (event in file.iterateXml(encodingInfo.charset, encodingInfo.bomSize)) {
                    val record = parser.processEvent(event)

                    if (record != null) {
                        yield(record)
                        processedCount++

                        if (processedCount % LOG_INTERVAL == 0) {
                            logger.info(
                                "Processed $processedCount records (path: ${rootPath.joinToString("/")}) " +
                                        "from ${file.toAbsolutePath()}"
                            )
                        }
                    } else if (parser.wasRecordSkipped()) {
                        skippedCount++
                    }
                }
            } catch (e: XmlParsingException) {
                // XmlParsingException уже содержит контекст из xml_events.kt
                logger.error("Error parsing file ${file.toAbsolutePath()}: ${e.message}", e)
                throw e
            } catch (e: Exception) {
                val errorContext = parser.getErrorContext()
                val message = "Error parsing file ${file.toAbsolutePath()} at record ${processedCount + 1}" +
                        (errorContext?.let { ", $it" } ?: "") +
                        ": ${e.message}"
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
 * Индекс конфигураций для быстрого поиска по относительному пути.
 */
private data class ConfigIndex(
    val byPath: Map<List<String>, List<XmlValueConfig>>,
    val maxDepth: Int
)

/**
 * Строит индекс конфигураций по относительным путям для оптимизации поиска.
 */
private fun buildConfigIndex(valueConfigs: Set<XmlValueConfig>): ConfigIndex {
    val configsByRelativePath = mutableMapOf<List<String>, MutableList<XmlValueConfig>>()
    var maxDepth = 0

    valueConfigs.forEach { config ->
        val targetRelativePath = when (config.valueType) {
            XmlValueType.ATTRIBUTE -> {
                // Для атрибутов: если путь содержит только имя атрибута,
                // то это атрибут самого rootPath элемента (пустой относительный путь)
                if (config.path.size == 1) {
                    emptyList()
                } else {
                    config.path.dropLast(1)
                }
            }
            // Для контента: путь как есть (может быть пустым для самого rootPath)
            XmlValueType.CONTENT -> config.path
        }

        maxDepth = maxOf(maxDepth, targetRelativePath.size)
        configsByRelativePath.getOrPut(targetRelativePath) { mutableListOf() }.add(config)
    }

    return ConfigIndex(configsByRelativePath, maxDepth)
}

/**
 * Парсер записей XML, управляющий состоянием обработки одной записи.
 * Инкапсулирует всю логику извлечения данных из событий StAX парсера.
 */
private class XmlRecordParser(
    private val rootPath: List<String>,
    private val configIndex: ConfigIndex,
    private val valueConfigs: Set<XmlValueConfig>,
    private val enumValues: Map<String, Set<String>>,
    private val validationOnlyKeys: Set<String>
) {
    // Текущий путь в XML дереве (абсолютный от корня документа)
    private val currentPath = mutableListOf<String>()

    // Стек для накопления текстового контента элементов
    private val contentStack = ArrayDeque<StringBuilder>()

    // Данные текущей обрабатываемой записи
    private var currentRecordData: MutableMap<String, String>? = null

    // Глубина, на которой находится rootPath элемент (-1 если мы вне его)
    private var rootElementDepth = -1

    // Флаг, указывающий что последняя запись была пропущена при валидации
    private var lastRecordSkipped = false

    /**
     * Обрабатывает одно событие StAX парсера.
     * @return Завершенную запись, если событие закрывает rootPath элемент, иначе null
     */
    fun processEvent(event: XmlEvent): Map<String, String>? {
        lastRecordSkipped = false

        return when (event) {
            is StartElementEvent -> {
                handleStartElement(event)
                null
            }
            is CharactersEvent -> {
                handleCharacters(event)
                null
            }
            is EndElementEvent -> {
                handleEndElement(event)
            }
        }
    }

    /**
     * Возвращает true, если последняя обработанная запись была пропущена.
     */
    fun wasRecordSkipped(): Boolean = lastRecordSkipped

    /**
     * Возвращает контекст для сообщения об ошибке.
     */
    fun getErrorContext(): String? {
        return if (currentPath.isNotEmpty()) {
            "current path: /${currentPath.joinToString("/")}"
        } else {
            null
        }
    }

    private fun handleStartElement(event: StartElementEvent) {
        currentPath.add(event.name)
        contentStack.addLast(StringBuilder())

        // Проверяем вход в rootPath элемент
        if (rootElementDepth == -1 && currentPath.size == rootPath.size && currentPath == rootPath) {
            rootElementDepth = currentPath.size
            currentRecordData = mutableMapOf()
        }

        // Извлекаем атрибуты, если мы внутри rootPath
        if (rootElementDepth > 0) {
            extractAttributes(event)
        }
    }

    private fun handleCharacters(event: CharactersEvent) {
        // Накапливаем контент только если мы внутри rootPath
        if (rootElementDepth > 0 && contentStack.isNotEmpty()) {
            val contentBuilder = contentStack.last()

            // Защита от переполнения памяти
            if (contentBuilder.length + event.content.length <= MAX_CONTENT_SIZE) {
                contentBuilder.append(event.content)
            } else if (contentBuilder.length < MAX_CONTENT_SIZE) {
                // Логируем только один раз при превышении лимита
                logger.warn(
                    "Content size exceeded $MAX_CONTENT_SIZE bytes for element '${event.name}' " +
                            "at path /${currentPath.joinToString("/")}, truncating..."
                )
                // Добавляем остаток до лимита
                val remaining = MAX_CONTENT_SIZE - contentBuilder.length
                contentBuilder.append(event.content.take(remaining))
            }
        }
    }

    private fun handleEndElement(event: EndElementEvent): Map<String, String>? {
        // Извлекаем контент закрываемого элемента
        val contentBuilder = contentStack.removeLastOrNull()
            ?: throw XmlParsingException(
                "Content stack is empty at EndElementEvent for '${event.name}' " +
                        "at path /${currentPath.joinToString("/")}"
            )

        // Извлекаем текстовое содержимое, если мы внутри rootPath
        if (rootElementDepth > 0) {
            extractContent(contentBuilder)
        }

        // Проверяем выход из rootPath элемента
        val result = if (currentPath.size == rootPath.size && currentPath == rootPath) {
            finalizeRecord()
        } else {
            null
        }

        // Удаляем элемент из пути
        currentPath.removeLastOrNull()
            ?: throw XmlParsingException("Current path is empty when processing EndElementEvent for '${event.name}'")

        return result
    }

    /**
     * Извлекает атрибуты из события начала элемента.
     */
    private fun extractAttributes(event: StartElementEvent) {
        val recordData = currentRecordData ?: return
        val relativeCurrentPath = getRelativePath()

        // Оптимизация: не ищем конфиги, если путь слишком глубокий
        if (relativeCurrentPath.size > configIndex.maxDepth) return

        configIndex.byPath[relativeCurrentPath]?.forEach { config ->
            if (config.valueType == XmlValueType.ATTRIBUTE) {
                // Имя атрибута - это последний элемент в config.path
                val attributeName = config.path.last()
                event.attributes[attributeName]?.let { value ->
                    recordData[config.outputKey] = value
                }
            }
        }
    }

    /**
     * Извлекает текстовое содержимое элемента.
     */
    private fun extractContent(contentBuilder: StringBuilder) {
        val recordData = currentRecordData ?: return
        val relativeClosedPath = getRelativePath()

        // Оптимизация: не ищем конфиги, если путь слишком глубокий
        if (relativeClosedPath.size > configIndex.maxDepth) return

        // Оптимизация: проверяем наличие конфигов перед trim()
        val contentConfigs = configIndex.byPath[relativeClosedPath]
            ?.filter { it.valueType == XmlValueType.CONTENT }

        if (!contentConfigs.isNullOrEmpty()) {
            val content = contentBuilder.toString().trim()
            if (content.isNotEmpty()) {
                contentConfigs.forEach { config ->
                    recordData[config.outputKey] = content
                }
            }
        }
    }

    /**
     * Завершает текущую запись, выполняет валидацию и фильтрацию.
     * @return Готовую запись для yield или null, если запись не прошла валидацию
     */
    private fun finalizeRecord(): Map<String, String>? {
        val recordData = currentRecordData ?: emptyMap()

        rootElementDepth = -1
        currentRecordData = null

        if (!isValidRecord(recordData)) {
            lastRecordSkipped = true
            return null
        }

        // Фильтруем поля, помеченные как validationOnly
        val resultData = if (validationOnlyKeys.isEmpty()) {
            recordData
        } else {
            recordData.filterKeys { key -> key !in validationOnlyKeys }
        }

        // Проверяем, что после фильтрации остались данные
        if (resultData.isEmpty()) {
            logger.debug("Record skipped: all fields are validationOnly at path ${rootPath.joinToString("/")}")
            lastRecordSkipped = true
            return null
        }

        return resultData
    }

    /**
     * Вычисляет относительный путь от rootPath до текущей позиции.
     */
    private fun getRelativePath(): List<String> {
        return if (currentPath.size > rootPath.size) {
            currentPath.subList(rootPath.size, currentPath.size)
        } else {
            emptyList()
        }
    }

    /**
     * Проверяет валидность записи согласно конфигурации.
     */
    private fun isValidRecord(result: Map<String, String>): Boolean {
        return valueConfigs.all { config ->
            val value = result[config.outputKey]

            // Проверка обязательных полей
            if (config.required && value.isNullOrBlank()) {
                logger.debug(
                    "Record invalid: required field '${config.outputKey}' missing or blank " +
                            "at path ${rootPath.joinToString("/")}"
                )
                return false
            }

            // Проверка enum значений (только если значение присутствует)
            if (value != null) {
                val enumSet = enumValues[config.outputKey]
                if (!enumSet.isNullOrEmpty() && value !in enumSet) {
                    logger.debug(
                        "Record invalid: value '$value' not in enum for '${config.outputKey}' " +
                                "at path ${rootPath.joinToString("/")}. " +
                                "Expected one of: ${enumSet.take(5).joinToString()}${if (enumSet.size > 5) "..." else ""}"
                    )
                    return false
                }
            }

            true
        }
    }
}
