package ru.my

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.math.BigDecimal
import java.sql.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.max
import kotlin.math.min
import kotlin.math.roundToInt

fun createDataSource(dbConfig: DbConfig): HikariDataSource {
    val config = HikariConfig().apply {
        jdbcUrl = dbConfig.jdbcUrl
        username = dbConfig.user
        password = dbConfig.password

        // ОСНОВНАЯ НАСТРОЙКА: уменьшаем пул, но не так радикально
        // Формула: Ядра * 2, но с жестким ограничением сверху.
        // Это дает параллелизм, но предотвращает бесконтрольный рост.
        val suggestedPoolSize = Runtime.getRuntime().availableProcessors() * 2
        maximumPoolSize = min(suggestedPoolSize, 16) // Не более 16 соединений!
        minimumIdle = max(2, maximumPoolSize / 4) // Динамический, но скромный

        connectionTimeout = dbConfig.props.connectionTimeout.toLong() * 1000
        idleTimeout = dbConfig.props.idleTimeout.toLong() * 1000
        maxLifetime = dbConfig.props.maxLifetime.toLong() * 1000
        validationTimeout = dbConfig.props.validationTimeout.toLong() * 1000

        addDataSourceProperty("socketTimeout", dbConfig.props.socketTimeout.toString())
        addDataSourceProperty("tcpKeepAlive", "true")
        addDataSourceProperty("reWriteBatchedInserts", "true")

        // statement_timeout должен сработать ДО socketTimeout, чтобы сервер успел прервать запрос
        val statementTimeout = (dbConfig.props.socketTimeout * 1000 * 0.85).roundToInt()
        addDataSourceProperty("options", "-c statement_timeout=$statementTimeout")
    }
    return HikariDataSource(config)
}

fun quoteDbIdent(name: String) = "\"${name.replace("\"", "\"\"")}\""

data class TableIdentifier(
    val name: String,
    val schema: String = "",
    val mapper: (String) -> String = { quoteDbIdent(it) },
) {
    init {
        require(name.isNotBlank()) { "Table name cannot be empty" }
    }

    val fullyQualifiedName: String get() {
        return sequenceOf(mapper(name), mapper(schema))
            .filter { it.isNotBlank() }
            .joinToString(".")
    }
}

data class ColumnInfo(
    val name: String,
    val type: Int,
    val isNullable: Boolean
) {
    constructor(rs: ResultSet): this(
        name = rs.getString("COLUMN_NAME"),
        type = rs.getInt("DATA_TYPE"),
        isNullable = rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable,
    )
}

//-----------------------------------------------------------------------------------------------------------
// EXTENSIONS
//-----------------------------------------------------------------------------------------------------------

fun <T> ResultSet.map(mapper: (ResultSet) -> T): Sequence<T> {
    require(!isClosed) { "ResultSet is closed" }

    return sequence {
        while (next()) {
            yield(mapper(this@map))
        }
    }
}

private val columnsCache = ConcurrentHashMap<TableIdentifier, List<ColumnInfo>>()

fun DatabaseMetaData.getColumnsInfo(table: TableIdentifier): List<ColumnInfo> {
    return columnsCache.getOrPut(table) {
        getColumns(null, table.schema, table.name, null).use { it.map(::ColumnInfo).toList() }
    }
}

inline fun <T> Connection.withTransaction(block: () -> T): T {
    val prevAutoCommit = autoCommit
    autoCommit = false
    return try {
        val result = block()
        commit()
        result
    } catch (e: Throwable) {
        rollback()
        throw e
    } finally {
        autoCommit = prevAutoCommit
    }
}

fun PreparedStatement.setParameter(index: Int, col: ColumnInfo, value: String?) {
    if (value == null) {
        if (col.isNullable) {
            setNull(index, col.type)
        } else {
            throw IllegalStateException("NOT NULL column '${col.name}' cannot be null")
        }
    } else {
        fun hexToBytes(hex: String): ByteArray {
            require(hex.length % 2 == 0) { "Hex string must have even length" }
            return hex.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
        }

        fun parseBoolean(value: String): Boolean = sequenceOf("1", "true").contains(value.lowercase())

        when (col.type) {
            Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
            Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR,
            Types.CLOB, Types.NCLOB, Types.ROWID -> setString(index, value)

            Types.BIT, Types.BOOLEAN -> setBoolean(index, parseBoolean(value))

            Types.TINYINT -> {
                try {
                    setByte(index, value.toByte())
                } catch (e: NumberFormatException) {
                    throw IllegalArgumentException("Cannot convert '$value' to TINYINT for column '${col.name}'", e)
                }
            }

            Types.SMALLINT -> {
                try {
                    setShort(index, value.toShort())
                } catch (e: NumberFormatException) {
                    throw IllegalArgumentException("Cannot convert '$value' to SMALLINT for column '${col.name}'", e)
                }
            }

            Types.INTEGER -> {
                try {
                    setInt(index, value.toInt())
                } catch (e: NumberFormatException) {
                    throw IllegalArgumentException("Cannot convert '$value' to INTEGER for column '${col.name}'", e)
                }
            }

            Types.BIGINT -> {
                try {
                    setLong(index, value.toLong())
                } catch (e: NumberFormatException) {
                    throw IllegalArgumentException("Cannot convert '$value' to BIGINT for column '${col.name}'", e)
                }
            }

            Types.FLOAT, Types.REAL -> {
                try {
                    setFloat(index, value.toFloat())
                } catch (e: NumberFormatException) {
                    throw IllegalArgumentException("Cannot convert '$value' to FLOAT for column '${col.name}'", e)
                }
            }

            Types.DOUBLE -> {
                try {
                    setDouble(index, value.toDouble())
                } catch (e: NumberFormatException) {
                    throw IllegalArgumentException("Cannot convert '$value' to DOUBLE for column '${col.name}'", e)
                }
            }

            Types.NUMERIC, Types.DECIMAL -> {
                try {
                    setBigDecimal(index, BigDecimal(value))
                } catch (e: NumberFormatException) {
                    throw IllegalArgumentException("Cannot convert '$value' to DECIMAL for column '${col.name}'", e)
                }
            }

            Types.DATE -> {
                try {
                    setDate(index, Date.valueOf(value))
                } catch (e: IllegalArgumentException) {
                    throw IllegalArgumentException(
                        "Cannot convert '$value' to DATE for column '${col.name}'. Expected format: YYYY-MM-DD",
                        e
                    )
                }
            }

            Types.TIME -> {
                try {
                    setTime(index, Time.valueOf(value))
                } catch (e: IllegalArgumentException) {
                    throw IllegalArgumentException(
                        "Cannot convert '$value' to TIME for column '${col.name}'. Expected format: HH:mm:ss",
                        e
                    )
                }
            }

            Types.TIMESTAMP -> {
                try {
                    val timestampValue = if (value.matches(Regex("\\d{4}-\\d{2}-\\d{2}"))) {
                        "$value 00:00:00" // Для Timestamp.valueOf важно чтобы время было указано
                    } else {
                        value
                    }
                    setTimestamp(index, Timestamp.valueOf(timestampValue))
                } catch (e: IllegalArgumentException) {
                    throw IllegalArgumentException(
                        "Cannot convert '$value' to TIMESTAMP for column '${col.name}'. Expected format: YYYY-MM-DD HH:mm:ss",
                        e
                    )
                }
            }

            Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> {
                try {
                    setBytes(index, hexToBytes(value))
                } catch (e: Exception) {
                    throw IllegalArgumentException(
                        "Cannot convert '$value' to BINARY for column '${col.name}'. Expected hex string",
                        e
                    )
                }
            }

            Types.OTHER, Types.JAVA_OBJECT, Types.DISTINCT, Types.STRUCT,
            Types.ARRAY, Types.REF, Types.DATALINK, Types.SQLXML,
            Types.REF_CURSOR, Types.TIME_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE ->
                throw UnsupportedOperationException("Type ${col.type} not supported for column '${col.name}'")

            else -> throw IllegalArgumentException("Unknown type: ${col.type} for column '${col.name}'")
        }
    }
}
