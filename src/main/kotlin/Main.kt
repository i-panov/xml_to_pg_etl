package ru.my

import kotlinx.cli.ArgParser
import kotlinx.cli.ArgType
import kotlinx.cli.required
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger
import kotlin.io.path.*

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

    val extractTo = extractDir?.let {
        Path(it).createDirectories().absolute()
    } ?: createTempDirectory("xml_to_pg_etl_").absolute()

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

    val xmlFile = Path(xmlPath)

    if (!xmlFile.exists()) {
        error("XML file, directory, or archive not found: $xmlPath")
    }

    runBlocking {
        val xmlFilesSequence = when {
            xmlFile.isDirectory() -> xmlFile.listDirectoryEntries("*.xml")
                .filter { it.isRegularFile() }.asSequence()
            xmlFile.isRegularFile() -> when {
                xmlFile.toString().endsWith(".xml", ignoreCase = true) -> sequenceOf(xmlFile)
                isArchive(xmlFile.toString()) -> extractArchive(xmlFile, extractTo)
                else -> emptySequence()
            }
            else -> emptySequence()
        }

        config.db.createDataSource().use { db ->
            val processedCount = AtomicInteger(0)
            val errorCount = AtomicInteger(0)

            xmlFilesSequence.asFlow()
                .flatMapMerge(concurrency = 4) { xml ->
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

                                db.connection.use { conn ->
                                    var batchCount = 0
                                    parseXmlElements(xml, mapping.xmlTag)
                                        .chunked(mapping.batchSize)
                                        .forEach { batch ->
                                            upserter.execute(batch)
                                            batchCount++
                                        }
                                    logger.info("Processed ${xml.fileName}: $batchCount batches")
                                }
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
                }
                .catch { e ->
                    // Логируем ошибку, но продолжаем обработку других файлов
                    logger.severe("Error in flow: ${e.message}")
                }
                .collect()

            val totalFiles = processedCount.get() + errorCount.get()
            if (totalFiles == 0) {
                logger.warning("No XML files found to process")
            } else {
                logger.info("Processing complete: $totalFiles files found, ${processedCount.get()} processed successfully, ${errorCount.get()} with errors")
            }
        }
    }
}
