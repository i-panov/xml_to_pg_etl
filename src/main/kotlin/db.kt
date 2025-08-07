package ru.my

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.math.BigDecimal
import java.sql.*

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
        leakDetectionThreshold = 60_000
        isAutoCommit = false
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

fun DatabaseMetaData.getColumnsInfo(table: String, schema: String? = null): List<ColumnInfo> =
    getColumns(null, schema, table, null).use { it.map(::ColumnInfo).toList() }

inline fun <T> Connection.withTransaction(block: () -> T): T {
    val prevAutoCommit = autoCommit
    autoCommit = false
    return try {
        val result = block()
        commit()
        result
    } catch (e: Exception) {
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
        when (col.type) {
            Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
            Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR,
            Types.CLOB, Types.NCLOB, Types.ROWID -> setString(index, value)

            Types.BIT, Types.BOOLEAN -> setBoolean(index, value.toBoolean())

            Types.TINYINT -> setByte(index, value.toByte())
            Types.SMALLINT -> setShort(index, value.toShort())
            Types.INTEGER -> setInt(index, value.toInt())
            Types.BIGINT -> setLong(index, value.toLong())

            Types.FLOAT, Types.REAL -> setFloat(index, value.toFloat())
            Types.DOUBLE -> setDouble(index, value.toDouble())

            Types.NUMERIC, Types.DECIMAL -> setBigDecimal(index, BigDecimal(value))

            Types.DATE -> setDate(index, Date.valueOf(value))
            Types.TIME -> setTime(index, Time.valueOf(value))

            Types.TIMESTAMP -> {
                val timestampValue = if (value.matches(Regex("\\d{4}-\\d{2}-\\d{2}"))) {
                    "$value 00:00:00" // Для Timestamp.valueOf важно чтобы время было указано
                } else {
                    value
                }
                setTimestamp(index, Timestamp.valueOf(timestampValue))
            }

            Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> setBytes(index, hexToBytes(value))

            Types.OTHER, Types.JAVA_OBJECT, Types.DISTINCT, Types.STRUCT,
            Types.ARRAY, Types.REF, Types.DATALINK, Types.SQLXML,
            Types.REF_CURSOR, Types.TIME_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE ->
                throw UnsupportedOperationException("Type ${col.type} not supported")

            else -> throw IllegalArgumentException("Unknown type: ${col.type}")
        }
    }
}

fun hexToBytes(hex: String): ByteArray {
    require(hex.length % 2 == 0) { "Hex string must have even length" }
    return hex.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
}
