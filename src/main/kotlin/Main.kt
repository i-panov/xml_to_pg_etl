package ru.my

import kotlinx.cli.*
import java.io.File
import java.util.logging.Logger

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

    parser.parse(args)

    logger.info("ENV file: $envPath")
    logger.info("XML file: $xmlPath")

    val config = loadAppConfig(envPath)
    val xmlFile = File(xmlPath)

    if (!xmlFile.exists()) {
        error("XML file not found: $xmlPath")
    }
}
