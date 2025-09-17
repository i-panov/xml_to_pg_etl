package ru.my

import kotlinx.cli.ArgParser
import kotlinx.cli.ArgType
import kotlinx.cli.required
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.*

enum class PathType { DIR, ARCHIVE, XML }

@OptIn(ExperimentalPathApi::class)
class XmlState(private val path: Path, extractDir: String?, private val fileMasks: Set<Regex> = emptySet()) {
    init {
        if (!path.exists()) {
            error("XML file, directory, or archive not found: $path")
        }
    }

    private val pathType = run {
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
    val logger = LoggerFactory.getLogger("Main")
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
            logger.error("$key: $value")
        }
        errors.isNotEmpty()
    }

    if (hasMappingErrors) {
        error("Has mapping errors")
    }

    val xmlState = XmlState(Path(xmlPath), extractDir, mappings.map { it.xmlFileRegex }.toSet())

    runBlocking {
        config.db.createDataSource().use { db ->
            val processedCount = AtomicInteger(0)
            val errorCount = AtomicInteger(0)
            val concurrency = Runtime.getRuntime().availableProcessors()

            xmlState.xmlFiles.flatMapMerge(concurrency) { xml ->
                flow {
                    val mapping = mappings.firstOrNull { m ->
                        m.xmlFileRegex.matches(xml.fileName.toString())
                    }

                    if (mapping != null) {
                        try {
                            val upserter = PostgresUpserter(
                                dataSource = db,
                                table = mapping.table,
                                uniqueColumns = mapping.uniqueColumns,
                            )

                            var batchCount = 0

                            // Заменяем Channel + launch на Flow + collect
                            val batchFlow = flow {
                                parseXmlElements(
                                    file = xml,
                                    tags = mapping.xmlTags,
                                    enumValues = mapping.enumValues,
                                ).chunked(mapping.batchSize).forEach { batch ->
                                        val mappedBatch = batch.map { item ->
                                            mapping.attributes.entries.mapNotNull { (tag, col) ->
                                                item[tag]?.let { value -> col to value }
                                            }.toMap()
                                        }
                                        emit(mappedBatch)
                                    }
                            }.flowOn(Dispatchers.IO).buffer(2) // ← парсинг в фоне + буферизация

                            batchFlow.collect { mappedBatch ->
                                withContext(Dispatchers.IO) { // ← вставка в БД не блокирует CPU-потоки
                                    upserter.execute(mappedBatch)
                                }
                                batchCount++
                            }

                            logger.info("Processed ${xml.fileName}: $batchCount batches")
                            processedCount.incrementAndGet()
                            emit(xml)
                        } catch (e: Exception) {
                            errorCount.incrementAndGet()
                            logger.error("Failed to process ${xml.fileName}: ${e.message}")
                            throw e
                        }
                    } else {
                        logger.warn("No mapping found for ${xml.fileName}")
                    }
                }
            }.catch { e ->
                logger.error("Error in flow: ${e.message}")
            }.collect()

            val totalFiles = processedCount.get() + errorCount.get()
            if (totalFiles == 0) {
                logger.warn("No XML files found to process")
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
