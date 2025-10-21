package ru.my

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.main
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import org.slf4j.LoggerFactory
import ru.my.db.PostgresUpserter
import ru.my.db.TableIdentifier
import ru.my.xml.isXml
import ru.my.xml.parseXmlElements
import java.nio.file.Path
import java.sql.SQLException
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource
import kotlin.io.path.*
import kotlin.system.exitProcess
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTime

enum class PathType { DIR, ARCHIVE, XML }

data class MappingWithFile(val xmlFile: Path, val mapping: MappingConfig)

class XmlState(
    val path: Path, extractDir: String?,
    private val fileMasks: Set<Regex> = emptySet(),
    val maxItemSizeBytes: Long = MAX_ARCHIVE_ITEM_SIZE,
) {
    init {
        require(path.exists()) { "XML file, directory, or archive not found: $path" }
    }

    val pathType = run {
        if (path.isDirectory()) {
            return@run PathType.DIR
        }

        if (path.isRegularFile()) {
            if (path.toString().endsWith(".xml", ignoreCase = true)) {
                return@run PathType.XML
            }

            if (path.isArchive()) {
                return@run PathType.ARCHIVE
            }
        }

        throw IllegalArgumentException("Unsupported xml path: $path")
    }

    private val pathToExtractArchive = when (pathType) {
        PathType.ARCHIVE, PathType.DIR -> extractDir?.let {
            Path(it).createDirectories().absolute()
        } ?: createTempDirectory("xml_to_pg_etl_").absolute()
        else -> null
    }

    val xmlFiles: Flow<Path> = when (pathType) {
        PathType.DIR -> flow {
            for (file in path.walk()) {
                if (file.isXml()) {
                    emit(file)
                } else if (file.isArchive()) {
                    val e = pathToExtractArchive!!.resolve(file.nameWithoutExtension)
                    emitAll(extractArchive(file, e).filter { it.isXml() })
                }
            }
        }
        PathType.XML -> flowOf(path)
        PathType.ARCHIVE -> extractArchive(path, pathToExtractArchive!!)
    }.buffer().flowOn(Dispatchers.IO)

    private fun extractArchive(path: Path, extractDir: Path) = extractArchive(
        archiveFile = path,
        extractDir = extractDir,
        maxItemSizeBytes = maxItemSizeBytes,
        checkerFileNameForExtract = { f ->
            val a = Path(f).fileName
            val b = a.toString()

            if (fileMasks.isEmpty()) {
                a.isXml()
            } else {
                fileMasks.any { it.matches(b) }
            }
        },
    )
}

@OptIn(ExperimentalCoroutinesApi::class)
class EtlCommand : CliktCommand() {
    private val envPath: String by option(
        "-e", "--env", help = "Path to env file",
    ).required()

    private val xmlPath: String by option(
        "-x", "--xml", help = "Path to XML files dir or archive",
    ).required()

    private val extractDir: String? by option(
        "-d", "--extract-dir", help = "Directory for archive extraction (temporary if not specified)",
    )

    private val config by lazy { AppConfig.load(Path(envPath)) }

    private val xmlState by lazy {
        XmlState(
            path = Path(xmlPath),
            extractDir = extractDir,
            fileMasks = config.mappings.map { it.xml.filesRegex }.flatten().toSet(),
            maxItemSizeBytes = config.maxArchiveItemSize,
        )
    }

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun run() = runBlocking {
        config.db.createDataSource().use { db ->
            val processedCount = AtomicInteger(0)
            val errorCount = AtomicInteger(0)
            val concurrency = Runtime.getRuntime().availableProcessors()

            val flow = xmlState.xmlFiles.mapNotNull { xml ->
                val fileName = xml.fileName.toString()

                val mapping = config.mappings.firstOrNull { m ->
                    m.xml.filesRegex.any { it.matches(fileName) }
                }

                if (mapping == null) {
                    logger.warn("No mapping found for ${xml.fileName}")
                    null
                } else {
                    MappingWithFile(xml, mapping)
                }
            }.flatMapMerge(concurrency) { (xml, mapping) ->
                flow {
                    try {
                        processXmlToMultipleTables(xml, mapping, db)

                        if (config.removeXmlAfterImport) {
                            logger.info("Removing processed XML: $xml")
                            xml.deleteIfExists()
                        }

                        processedCount.incrementAndGet()
                        emit(xml)
                    } catch (e: Exception) {
                        errorCount.incrementAndGet()
                        logger.error("Failed to process ${xml.fileName}", e)

                        if (config.stopOnError) {
                            throw RuntimeException(
                                "Stop on error flag is set. Terminating due to error in file: ${xml.fileName}",
                                e
                            )
                        }
                    }
                }
            }

            try {
                flow.collect()

                if (config.removeArchivesAfterUnpack && xmlState.pathType == PathType.ARCHIVE) {
                    logger.info("All files from archive processed. Removing archive: ${xmlState.path}")
                    xmlState.path.deleteIfExists()
                }
            } catch (e: Exception) {
                logger.error("ETL process was terminated due to an error", e)
                exitProcess(1)
            }

            val totalFiles = processedCount.get() + errorCount.get()
            if (totalFiles == 0) {
                logger.warn("No XML files found to process")
            } else {
                logger.info(
                    "Processing complete: $totalFiles files found, " +
                            "${processedCount.get()} processed successfully, " +
                            "${errorCount.get()} with errors"
                )
            }
        }
    }

    private suspend fun processXmlToMultipleTables(
        xml: Path,
        mapping: MappingConfig,
        db: DataSource
    ) = coroutineScope {
        val upserters = mapping.iteratePostgresUpserters(db).toList()

        // Создаем каналы для каждой таблицы
        data class TableProcessor(
            val upserter: PostgresUpserter,
            val channel: Channel<Map<String, String>>,
            val batchSize: Int
        )

        val processors = upserters.map { upserter ->
            val dbMapping = mapping.db[upserter.table]!!
            TableProcessor(
                upserter = upserter,
                channel = Channel(capacity = (dbMapping.batchSize * 2).coerceAtMost(1000)), // Буфер на 2 батча
                batchSize = dbMapping.batchSize
            )
        }

        // Producer: парсим XML и распределяем по каналам
        val producerJob = launch(Dispatchers.IO) {
            runCatching {
                val sequence = parseXmlElements(
                    file = xml,
                    valueConfigs = mapping.xml.values.entries
                        .map { (k, v) -> v.toXmlValueConfig(k) }.toSet(),
                    rootPath = mapping.xml.rootPath,
                    enumValues = mapping.xml.enums,
                )

                for (row in sequence) {
                    // Распределяем строку по таблицам
                    for (processor in processors) {
                        val tableRow = buildMap {
                            for ((originalKey, value) in row) {
                                val columnId = mapping.resolvedColumnMapping[originalKey] ?: continue

                                if (columnId.table == processor.upserter.table) {
                                    // Используем только имя колонки без префикса
                                    put(columnId.name, value)
                                }
                            }
                        }

                        if (tableRow.isNotEmpty()) {
                            processor.channel.send(tableRow)
                        }
                    }
                }
            }.also {
                // Закрываем все каналы
                processors.forEach { it.channel.close() }
            }
        }

        // Consumers: читаем из каналов и пишем в БД параллельно
        val consumerJobs = processors.map { processor ->
            async(Dispatchers.IO) {
                val table = processor.upserter.table
                logger.info("Starting processing ${xml.fileName} -> $table. Query: ${processor.upserter.sql}")

                var batchCount = 0
                var rowsProcessed = 0
                val batch = mutableListOf<Map<String, String>>()

                try {
                    for (row in processor.channel) {
                        batch.add(row)

                        if (batch.size >= processor.batchSize) {
                            val duration = measureTime {
                                flow {
                                    emit(processor.upserter.execute(batch.toList()))
                                }.retry(3) { e ->
                                    val msg = e.message

                                    if (e is SQLException && msg != null) {
                                        if (RETRY_ERRORS.any { msg.contains(it) }) {
                                            delay(3.seconds)
                                            logger.warn("Retrying SQL error: $msg")
                                            return@retry true
                                        }
                                    }

                                    false
                                }.single()
                            }

                            batchCount++
                            rowsProcessed += batch.size
                            batch.clear()

                            if (duration > 60.seconds) {
                                logger.debug(
                                    "Batch executed in {} (table: {}, size: {})",
                                    duration,
                                    table,
                                    batch.size
                                )
                            }
                        }
                    }

                    if (batch.isNotEmpty()) {
                        processor.upserter.execute(batch)
                        batchCount++
                        rowsProcessed += batch.size
                    }

                    logger.info("Completed ${xml.fileName} -> $table: $batchCount batches, $rowsProcessed rows")

                    TableLoadResult(table, success = true, rowsProcessed = rowsProcessed)
                } catch (e: Exception) {
                    logger.error("Failed to load ${xml.fileName} -> $table", e)
                    TableLoadResult(table, success = false, error = e)
                }
            }
        }

        producerJob.join()
        val results = consumerJobs.awaitAll()

        val failed = results.filter { !it.success }
        if (failed.isNotEmpty()) {
            val errorMsg = failed.joinToString(", ") { "${it.table}: ${it.error?.message}" }
            throw RuntimeException("Failed to load into tables: $errorMsg")
        }

        val totalRows = results.sumOf { it.rowsProcessed }
        logger.info("Successfully loaded ${xml.fileName}: $totalRows total rows across ${results.size} tables")
    }

    private data class TableLoadResult(
        val table: TableIdentifier,
        val success: Boolean,
        val rowsProcessed: Int = 0,
        val error: Throwable? = null
    )

    companion object {
        private val RETRY_ERRORS = setOf("I/O error", "socket", "timeout", "connection")
    }
}

fun main(args: Array<String>) = EtlCommand().main(args)
