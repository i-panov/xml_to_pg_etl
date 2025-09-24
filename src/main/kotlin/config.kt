package ru.my

import com.akuleshov7.ktoml.Toml
import com.akuleshov7.ktoml.source.decodeFromStream
import com.zaxxer.hikari.HikariDataSource
import java.io.File
import java.nio.file.Path
import kotlin.io.path.inputStream

data class DbProps(
    val connectionTimeout: Int = 30,
    val idleTimeout: Int = 2 * 60,
    val maxLifetime: Int = 20 * 60,
    val validationTimeout: Int = 5,
    val socketTimeout: Int = 15 * 60,
) {
    init {
        require(connectionTimeout >= 0) { "Connection timeout must be non-negative" }
        require(idleTimeout >= 0) { "Idle timeout must be non-negative" }
        require(maxLifetime >= 0) { "Max lifetime must be non-negative" }
        require(validationTimeout >= 0) { "Validation timeout must be non-negative" }
        require(socketTimeout >= 0) { "Socket timeout must be non-negative" }
    }
}

data class DbConfig(
    val host: String,
    val port: Int = 5432,
    val user: String,
    val password: String,
    val database: String,
    val props: DbProps = DbProps(),
) {
    val jdbcUrl = "jdbc:postgresql://${host}:${port}/${database}"

    fun createDataSource(): HikariDataSource = createDataSource(this)

    init {
        require(host.isNotBlank()) { "DB host cannot be blank" }
        require(port in 1..65535) { "DB port must be between 1 and 65535" }
        require(user.isNotBlank()) { "DB user cannot be blank" }
        require(database.isNotBlank()) { "DB database cannot be blank" }
    }
}

data class AppConfig(
    val db: DbConfig,
    val mappingsFile: String,
    val removeArchivesAfterUnpack: Boolean = false,
    val removeXmlAfterImport: Boolean = false,
    val stopOnError: Boolean = false,
    val maxArchiveItemSize: Long = MAX_ARCHIVE_ITEM_SIZE,
) {
    fun loadMappings(): List<MappingConfig> {
        val file = File(mappingsFile)

        if (!file.exists()) {
            throw IllegalArgumentException("Mappings file not exists: $mappingsFile")
        }

        return MappingConfig.parseItems(file)
    }

    init {
        require(mappingsFile.isNotBlank()) { "Mappings file path cannot be blank" }
    }
}

fun loadAppConfig(path: Path): AppConfig = path.inputStream().use { Toml.decodeFromStream<AppConfig>(it) }
