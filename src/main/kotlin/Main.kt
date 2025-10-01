package ru.my

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.main
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.*
import kotlin.system.exitProcess

enum class PathType { DIR, ARCHIVE, XML }

data class MappingWithFile(val xmlFile: Path, val mapping: MappingConfig)

class XmlState(
    val path: Path, extractDir: String?,
    private val fileMasks: Set<Regex> = emptySet(),
    maxItemSizeBytes: Long = MAX_ARCHIVE_ITEM_SIZE,
) {
    init {
        if (!path.exists()) {
            error("XML file, directory, or archive not found: $path")
        }
    }

    val pathType = run {
        if (path.isDirectory()) {
            return@run PathType.DIR
        }

        if (path.isRegularFile()) {
            if (path.toString().endsWith(".xml", ignoreCase = true)) {
                return@run PathType.XML
            }

            if (isArchive(path.toString())) {
                return@run PathType.ARCHIVE
            }
        }

        throw IllegalArgumentException("Unsupported xml path: $path")
    }

    private val pathToExtractArchive = when (pathType) {
        PathType.ARCHIVE -> extractDir?.let {
            Path(it).createDirectories().absolute()
        } ?: createTempDirectory("xml_to_pg_etl_").absolute()
        else -> null
    }

    val xmlFiles: Flow<Path> = when (pathType) {
        PathType.DIR -> flow {
            for (file in path.walk()) {
                if (file.extension.equals("xml", true) && file.isRegularFile()) {
                    emit(file)
                }
            }
        }
        PathType.XML -> flowOf(path)
        PathType.ARCHIVE -> extractArchive(
            archiveFile = path,
            extractDir = pathToExtractArchive!!,
            maxItemSizeBytes = maxItemSizeBytes,
            checkerFileNameForExtract = { f ->
                val b = Path(f).fileName.toString()

                if (fileMasks.isEmpty()) {
                    isXmlFile(b)
                } else {
                    fileMasks.any { it.matches(b) }
                }
            },
        )
    }.buffer().flowOn(Dispatchers.IO)
}

@OptIn(ExperimentalCoroutinesApi::class)
class EtlCommand : CliktCommand() {
    private val envPath: String by option(
        "-e", "--env", help = "Path to env file",
    ).required()

    private val xmlPath: String by option(
        "-x", "--xml", help = "Path to XML files dir or archive",
    ).required()

    private val extractDir: String by option(
        "-d", "--extract-dir", help = "Directory for archive extraction (temporary if not specified)",
    ).required()

    private val config by lazy { AppConfig.load(Path(envPath)) }

    private val xmlState by lazy { XmlState(
        path = Path(xmlPath),
        extractDir = extractDir,
        fileMasks = config.mappings.map { it.xml.filesRegex }.flatten().toSet(),
        maxItemSizeBytes = config.maxArchiveItemSize,
    ) }

    private val logger = LoggerFactory.getLogger(::javaClass.get())

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
            }.onCompletion { cause ->
                if (config.removeArchivesAfterUnpack && xmlState.pathType == PathType.ARCHIVE && cause == null) {
                    logger.info("All files from archive processed. Removing archive: ${xmlState.path}")
                    xmlState.path.deleteIfExists()
                }
            }.flatMapMerge(concurrency) { (xml, mapping) ->
                flow {
                    try {
                        val upserter = PostgresUpserter(
                            dataSource = db,
                            table = TableIdentifier(mapping.db.table, mapping.db.schema ?: ""),
                            targetColumns = mapping.xml.values.filter { !it.value.notForSave }.keys,
                            uniqueColumns = mapping.db.uniqueColumns,
                        )

                        logger.info("Starting processing ${xml.fileName}. Query: ${upserter.sql}")

                        var batchCount = 0

                        val batchFlow = flow {
                            parseXmlElements(
                                file = xml,
                                valueConfigs = mapping.xml.values.entries
                                    .map { (k, v) -> v.toXmlValueConfig(k) }.toSet(),
                                rootPath = mapping.xml.rootPath,
                                enumValues = mapping.xml.enums,
                            ).chunked(mapping.db.batchSize).forEach { batch -> emit(batch) }
                        }.flowOn(Dispatchers.IO).buffer(2)

                        batchFlow.collect { mappedBatch ->
                            withContext(Dispatchers.IO) {
                                val startTime = System.currentTimeMillis()
                                upserter.execute(mappedBatch)
                                val duration = System.currentTimeMillis() - startTime
                                if (duration > 60 * 1000) { // Логируем только долгие батчи
                                    logger.debug("Batch executed in ${duration}ms (size: ${mappedBatch.size})")
                                }
                            }
                            batchCount++
                        }

                        logger.info("Processed ${xml.fileName}: $batchCount batches")

                        if (config.removeXmlAfterImport) {
                            logger.info("Removing processed XML: $xml")
                            xml.deleteIfExists()
                        }

                        processedCount.incrementAndGet()
                        emit(xml)
                    } catch (e: Exception) {
                        errorCount.incrementAndGet()
                        logger.error("Failed to process ${xml.fileName}: ${e.message}")

                        if (config.stopOnError) {
                            throw RuntimeException("Stop on error flag is set. Terminating due to error in file: ${xml.fileName}", e)
                        }
                    }
                }
            }

            try {
                flow.collect()
            } catch (e: Exception) {
                logger.error("ETL process was terminated due to an error: ${e.message}")
                exitProcess(1)
            }

            val totalFiles = processedCount.get() + errorCount.get()
            if (totalFiles == 0) {
                logger.warn("No XML files found to process")
            } else {
                logger.info("Processing complete: $totalFiles files found, ${processedCount.get()} processed successfully, ${errorCount.get()} with errors")
            }
        }
    }
}

fun main(args: Array<String>) = EtlCommand().main(args)
