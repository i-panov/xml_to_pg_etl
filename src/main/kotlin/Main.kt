package ru.my

import kotlinx.cli.ArgParser
import kotlinx.cli.ArgType
import kotlinx.cli.required
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger
import kotlin.io.path.*

enum class PathType { DIR, ARCHIVE, XML }

@OptIn(ExperimentalPathApi::class)
class XmlState(val path: Path, val extractDir: String?) {
    init {
        if (!path.exists()) {
            error("XML file, directory, or archive not found: $path")
        }
    }

    val pathType: PathType = when {
        path.isDirectory() -> PathType.DIR
        path.isRegularFile() -> when {
            path.toString().endsWith(".xml", ignoreCase = true) -> PathType.XML
            isArchive(path.toString()) -> PathType.ARCHIVE
            else -> throw IllegalArgumentException("Unsupported xml path: $path")
        }
        else -> throw IllegalArgumentException("Unsupported xml path: $path")
    }

    val pathToExtractArchive = when (pathType) {
        PathType.ARCHIVE -> extractDir?.let {
            Path(it).createDirectories().absolute()
        } ?: createTempDirectory("xml_to_pg_etl_").absolute()
        else -> null
    }

    val xmlFiles: Sequence<Path> = when (pathType) {
        PathType.DIR -> path.walk().filter {
            it.extension.equals("xml", true) && it.isRegularFile()
        }
        PathType.XML -> sequenceOf(path)
        PathType.ARCHIVE -> extractArchive(path, pathToExtractArchive!!) // todo: распараллелить разархивацию
    }

    fun removeArchive() {
        if (pathType == PathType.ARCHIVE) {
            path.deleteIfExists()
        }
    }

    fun removeXml(): Any = when (pathType) {
        PathType.DIR -> path.deleteRecursively()
        PathType.XML -> path.deleteIfExists()
        PathType.ARCHIVE -> pathToExtractArchive?.deleteRecursively() ?: Unit
    }
}

@OptIn(ExperimentalCoroutinesApi::class)
fun main(args: Array<String>) {
    val logger = Logger.getLogger("Main")
    val parser = ArgParser("XML_TO_PG_ETL")

    val envPath by parser.option(ArgType.String,
        fullName = "env",
        shortName = "e",
        description = "Path to .env file",
    ).required()

    val xmlPath by parser.option(ArgType.String,
        fullName = "xml",
        shortName = "x",
        description = "Path to XML files dir or archive",
    ).required()

    val extractDir by parser.option(ArgType.String,
        fullName = "extract-dir",
        shortName = "d",
        description = "Directory for archive extraction (temporary if not specified)",
    )

    parser.parse(args)

    logger.info("ENV file: $envPath")
    logger.info("XML file: $xmlPath")

    val config = loadAppConfig(Path(envPath)).apply { validate() }
    val mappings = config.loadMappings()

    val hasMappingErrors = mappings.any { mapping ->
        val errors = mapping.validate()
        errors.forEach { (key, value) ->
            logger.severe("$key: $value")
        }
        errors.isNotEmpty()
    }

    if (hasMappingErrors) {
        error("Has mapping errors")
    }

    val xmlState = XmlState(Path(xmlPath), extractDir)

    runBlocking {
        config.db.createDataSource().use { db ->
            val processedCount = AtomicInteger(0)
            val errorCount = AtomicInteger(0)

            xmlState.xmlFiles.asFlow().flatMapMerge(concurrency = 4) { xml ->
                flow {
                    val mapping = mappings.firstOrNull { m ->
                        Regex(m.xmlFile).matches(xml.fileName.toString())
                    }

                    if (mapping != null) {
                        try {
                            val upserter = PostgresUpserter(
                                dataSource = db,
                                table = mapping.table,
                                schema = mapping.schema,
                                uniqueColumns = mapping.uniqueColumns,
                            )

                            var batchCount = 0

                            // Pipeline: парсинг и вставка в параллельных корутинах
                            val batchChannel = Channel<List<Map<String, String>>>(capacity = 2)

                            // Producer - парсит XML и готовит batch'и
                            val producerJob = launch {
                                try {
                                    parseXmlElements(xml, mapping.xmlTag)
                                        .chunked(mapping.batchSize)
                                        .forEach { batch ->
                                            val mappedBatch = batch.map { item ->
                                                mapping.attributes.entries.mapNotNull { (tag, col) ->
                                                    item[tag]?.let { value -> col to value }
                                                }.toMap()
                                            }
                                            batchChannel.send(mappedBatch)
                                        }
                                } finally {
                                    batchChannel.close()
                                }
                            }

                            // Consumer - забирает готовые batch'и и вставляет в БД
                            for (mappedBatch in batchChannel) {
                                upserter.execute(mappedBatch)
                                batchCount++
                            }

                            // Ждем завершения producer'а (на всякий случай)
                            producerJob.join()

                            logger.info("Processed ${xml.fileName}: $batchCount batches")
                            processedCount.incrementAndGet()
                            emit(xml)
                        } catch (e: Exception) {
                            errorCount.incrementAndGet()
                            logger.severe("Failed to process ${xml.fileName}: ${e.message}")
                            throw e // перебросим для обработки ниже
                        }
                    } else {
                        logger.warning("No mapping found for ${xml.fileName}")
                    }
                }
            }.catch { e ->
                // Логируем ошибку, но продолжаем обработку других файлов
                logger.severe("Error in flow: ${e.message}")
            }.collect()

            val totalFiles = processedCount.get() + errorCount.get()
            if (totalFiles == 0) {
                logger.warning("No XML files found to process")
            } else {
                logger.info("Processing complete: $totalFiles files found, ${processedCount.get()} processed successfully, ${errorCount.get()} with errors")
            }
        }
    }

    if (config.removeArchivesAfterUnpack) {
        xmlState.removeArchive()
    }

    if (config.removeXmlAfterImport) {
        xmlState.removeXml()
    }
}
