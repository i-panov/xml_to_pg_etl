package ru.my

import kotlinx.cli.ArgParser
import kotlinx.cli.ArgType
import kotlinx.cli.required
import java.util.logging.Logger
import kotlin.io.path.*

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

    val xmlFiles = when {
        xmlFile.isRegularFile() -> {
            when {
                isArchive(xmlFile.toString()) -> {
                    extractArchive(xmlFile, extractTo).toList()
                }
                xmlFile.toString().endsWith(".xml", ignoreCase = true) -> {
                    listOf(xmlFile)
                }
                else -> emptyList()
            }
        }
        xmlFile.isDirectory() -> {
            xmlFile.listDirectoryEntries("*.xml")
                .filter { it.isRegularFile() }
        }
        else -> emptyList()
    }
}
