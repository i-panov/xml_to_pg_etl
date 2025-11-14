package ru.my

import com.akuleshov7.ktoml.Toml
import com.akuleshov7.ktoml.source.decodeFromStream
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.serialization.Serializable
import org.postgresql.PGProperty
import java.io.File
import java.nio.file.Path
import kotlin.io.path.inputStream
import kotlin.math.max
import kotlin.math.min
import kotlin.math.roundToInt

@Serializable
data class DbProps(
    val connectionTimeout: Int = 30,
    val idleTimeout: Int = 2 * 60,
    val maxLifetime: Int = 20 * 60,
    val validationTimeout: Int = 5,
    val socketTimeout: Int = 15 * 60,
    val schema: String = "",
    val appName: String = "",
) {
    init {
        require(connectionTimeout >= 0) { "Connection timeout must be non-negative" }
        require(idleTimeout >= 0) { "Idle timeout must be non-negative" }
        require(maxLifetime >= 0) { "Max lifetime must be non-negative" }
        require(validationTimeout >= 0) { "Validation timeout must be non-negative" }
        require(socketTimeout >= 0) { "Socket timeout must be non-negative" }
    }
}

@Serializable
data class DbConfig(
    val host: String,
    val port: Int = 5432,
    val user: String,
    val password: String,
    val database: String,
    val props: DbProps = DbProps(),
) {
    val propsString by lazy {
        sequenceOf(
            "ApplicationName" to props.appName,
        ).filter { it.second.isNotBlank() }
            .joinToString("&") { "${it.first}=${it.second}" }
            .let { if (it.isNotBlank()) "?$it" else "" }
    }

    val jdbcUrl by lazy {
        "jdbc:postgresql://${host}:${port}/${database}$propsString"
    }

    init {
        require(host.isNotBlank()) { "DB host cannot be blank" }
        require(port in 1..65535) { "DB port must be between 1 and 65535" }
        require(user.isNotBlank()) { "DB user cannot be blank" }
        require(database.isNotBlank()) { "DB database cannot be blank" }
    }

    fun createDataSource(): HikariDataSource {
        val config = HikariConfig().apply {
            jdbcUrl = this@DbConfig.jdbcUrl
            username = this@DbConfig.user
            password = this@DbConfig.password
            schema = props.schema.ifBlank { null }

            // ОСНОВНАЯ НАСТРОЙКА: уменьшаем пул, но не так радикально
            // Формула: Ядра * 2, но с жестким ограничением сверху.
            // Это дает параллелизм, но предотвращает бесконтрольный рост.
            val suggestedPoolSize = Runtime.getRuntime().availableProcessors() * 2 + 1
            maximumPoolSize = min(suggestedPoolSize, 16) // Не более 16 соединений!
            minimumIdle = max(2, maximumPoolSize / 4) // Динамический, но скромный

            connectionTimeout = props.connectionTimeout.toLong() * 1000
            idleTimeout = props.idleTimeout.toLong() * 1000
            maxLifetime = props.maxLifetime.toLong() * 1000
            validationTimeout = props.validationTimeout.toLong() * 1000

            setDataSourceProperty(PGProperty.SOCKET_TIMEOUT, props.socketTimeout)
            setDataSourceProperty(PGProperty.TCP_KEEP_ALIVE, true)
            setDataSourceProperty(PGProperty.REWRITE_BATCHED_INSERTS, true)

            // statement_timeout должен сработать ДО socketTimeout, чтобы сервер успел прервать запрос
            val statementTimeout = (props.socketTimeout * 1000 * 0.85).roundToInt()

            connectionInitSql = "set local statement_timeout = $statementTimeout"
        }

        return HikariDataSource(config)
    }
}

@Serializable
data class AppConfig(
    val db: DbConfig,
    val mappingsFile: String,
    val removeArchivesAfterUnpack: Boolean = false,
    val removeXmlAfterImport: Boolean = false,
    val stopOnError: Boolean = false,
    val maxArchiveItemSize: Long = MAX_ARCHIVE_ITEM_SIZE,
) {
    val mappings by lazy {
        val file = File(mappingsFile)

        if (!file.exists()) {
            throw IllegalArgumentException("Mappings file not exists: $mappingsFile")
        }

        MappingConfig.parseItems(file)
    }

    init {
        require(mappingsFile.isNotBlank()) { "Mappings file path cannot be blank" }
    }

    companion object {
        fun load(path: Path): AppConfig = path.inputStream().use { Toml.decodeFromStream<AppConfig>(it) }
    }
}

/**
 * Типобезопасный хелпер для установки свойств PostgreSQL драйвера в HikariConfig.
 * @param property Свойство из энума `PGProperty`.
 * @param value Значение свойства.
 */
fun HikariConfig.setDataSourceProperty(property: PGProperty, value: Any) {
    // Используем именно .getName(), чтобы получить правильное имя свойства ("socketTimeout"),
    // а не .name, которое вернет имя enum-константы ("SOCKET_TIMEOUT").
    @Suppress("UsePropertyAccessSyntax")
    addDataSourceProperty(property.getName(), value.toString())
}
