package ru.my

import java.util.logging.Logger
import java.nio.file.Path
import kotlin.io.path.*

data class DbConfig(
    val host: String,
    val port: Int = 5432,
    val user: String,
    val password: String,
    val database: String,
    val schema: String? = null,
)

data class AppConfig(
    val db: DbConfig,
    val mappingsFile: String,
    val removeArchivesAfterUnpack: Boolean = false,
    val removeXmlAfterImport: Boolean = false,
)

private val logger = Logger.getLogger("AppConfigLoader")

fun parseEnvFile(path: Path): Map<String, String> {
    if (!path.exists()) {
        throw IllegalArgumentException("Environment file not found: $path")
    }

    return path.readLines()
        .mapIndexedNotNull { index, line ->
            val trimmed = line.trim()
            when {
                trimmed.isEmpty() || trimmed.startsWith("#") -> null
                "=" !in trimmed -> {
                    logger.warning("Warning: Invalid format at line ${index + 1}: $line")
                    null
                }
                else -> {
                    val (key, value) = trimmed.split("=", limit = 2)
                    // Убираем кавычки если есть
                    val cleanValue = value.trim().removeSurrounding("\"").removeSurrounding("'")
                    key.trim() to cleanValue
                }
            }
        }.toMap()
}

fun loadAppConfig(envPath: Path): AppConfig {
    logger.info("Loading configuration from: $envPath")
    val env = parseEnvFile(envPath)

    val db = DbConfig(
        host = env["DB_HOST"] ?: error("DB_HOST is required"),
        port = env["DB_PORT"]?.toIntOrNull()?.takeIf { it in 1..65535 }
            ?: 5432,
        user = env["DB_USER"] ?: error("DB_USER is required"),
        password = env["DB_PASSWORD"] ?: error("DB_PASSWORD is required"),
        database = env["DB_DATABASE"] ?: error("DB_DATABASE is required"),
        schema = env["DB_SCHEMA"] // nullable
    )

    val cfg = AppConfig(
        db = db,
        mappingsFile = env["MAPPINGS_FILE"] ?: error("MAPPINGS_FILE is required"),
        removeArchivesAfterUnpack = env["REMOVE_ARCHIVES_AFTER_UNPACK"]?.toBoolean() ?: false,
        removeXmlAfterImport = env["REMOVE_XML_AFTER_IMPORT"]?.toBoolean() ?: false,
    );

    logger.info("Configuration loaded: DB=${db.host}:${db.port}/${db.database}")
    return cfg
}
