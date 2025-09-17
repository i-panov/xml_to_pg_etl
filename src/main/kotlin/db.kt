package ru.my

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.math.BigDecimal
import java.sql.*
import java.util.concurrent.ConcurrentHashMap

fun createDataSource(dbConfig: DbConfig): HikariDataSource {
    val config = HikariConfig().apply {
        jdbcUrl = dbConfig.jdbcUrl
        username = dbConfig.user
        password = dbConfig.password

        // Оптимальные настройки для высокой нагрузки
        maximumPoolSize = Runtime.getRuntime().availableProcessors() * 4
        minimumIdle = maximumPoolSize / 2
        connectionTimeout = 30_000
        idleTimeout = 600_000
        maxLifetime = 1_800_000
        validationTimeout = 3_000
        leakDetectionThreshold = 10 * 60 * 1000
    }
    return HikariDataSource(config)
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

fun <T> ResultSet.map(mapper: (ResultSet) -> T): Sequence<T> = sequence {
    while (next()) {
        yield(mapper(this@map))
    }
}

private val columnsCache = ConcurrentHashMap<Pair<String?, String>, List<ColumnInfo>>()

fun DatabaseMetaData.getColumnsInfo(table: String, schema: String? = null): List<ColumnInfo> {
    return columnsCache.getOrPut(schema to table) {
        getColumns(null, schema, table, null).use { it.map(::ColumnInfo).toList() }
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
