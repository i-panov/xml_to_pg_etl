package ru.my.db

import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.KeyDeserializer
import java.math.BigDecimal
import java.sql.*
import java.util.concurrent.ConcurrentHashMap

fun quoteDbIdent(name: String) = "\"${name.replace("\"", "\"\"")}\""

data class TableIdentifier(
    val name: String,
    val schema: String = "",
) {
    init {
        require(name.isNotBlank()) { "Table name cannot be empty" }
        require(name.length <= 63) { "Table name too long (max 63 characters)" }
        require(schema.length <= 63) { "Schema name too long (max 63 characters)" }
    }

    companion object {
        fun parse(str: String): TableIdentifier {
            val parts = str.split(".")
            require(parts.isNotEmpty()) { "Invalid table identifier: $str" }

            return if (parts.size == 1) {
                TableIdentifier(parts[0])
            } else {
                TableIdentifier(parts[1], parts[0])
            }
        }
    }

    val fullyQualifiedName by lazy {
        sequenceOf(schema, name)
            .filter { it.isNotBlank() }
            .map { quoteDbIdent(it) }
            .joinToString(".")
    }

    override fun toString(): String = fullyQualifiedName
}

class TableIdentifierKeyDeserializer : KeyDeserializer() {
    override fun deserializeKey(key: String, ctxt: DeserializationContext): TableIdentifier {
        return TableIdentifier.parse(key)
    }
}

data class ColumnInfo(
    val name: String,
    val typeId: Int,
    val typeName: String, // может быть полезно например для JSON, для которого typeId будет OTHER
    val isNullable: Boolean
) {
    constructor(rs: ResultSet): this(
        name = rs.getString("COLUMN_NAME"),
        typeId = rs.getInt("DATA_TYPE"),
        typeName = rs.getString("TYPE_NAME").lowercase(),
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
    return columnsCache.computeIfAbsent(table) {
        getColumns(null, table.schema, table.name, null).use {
            it.map(::ColumnInfo).toList()
        }
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
            setNull(index, col.typeId)
        } else {
            throw IllegalStateException("NOT NULL column '${col.name}' cannot be null")
        }
    } else {
        fun hexToBytes(hex: String): ByteArray {
            require(hex.length % 2 == 0) { "Hex string must have even length" }
            return hex.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
        }

        when (col.typeId) {
            Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
            Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR,
            Types.CLOB, Types.NCLOB, Types.ROWID -> setString(index, value)

            Types.BIT, Types.BOOLEAN -> {
                val isTrue = sequenceOf("1", "true", "t", "yes", "y").contains(value.lowercase())
                setBoolean(index, isTrue)
            }

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
                    setFloat(index, (value.replace(',', '.')).toFloat())
                } catch (e: NumberFormatException) {
                    throw IllegalArgumentException("Cannot convert '$value' to FLOAT for column '${col.name}'", e)
                }
            }

            Types.DOUBLE -> {
                try {
                    setDouble(index, (value.replace(',', '.')).toDouble())
                } catch (e: NumberFormatException) {
                    throw IllegalArgumentException("Cannot convert '$value' to DOUBLE for column '${col.name}'", e)
                }
            }

            Types.NUMERIC, Types.DECIMAL -> {
                try {
                    setBigDecimal(index, BigDecimal(value.replace(',', '.')))
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
                throw UnsupportedOperationException("Type ${col.typeId} not supported for column '${col.name}'")

            else -> throw IllegalArgumentException("Unknown type: ${col.typeId} for column '${col.name}'")
        }
    }
}
