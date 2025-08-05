package ru.my

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.PreparedStatement
import java.sql.ResultSet

data class ColumnInfo(
    val name: String,
    val type: Int,
    val isNullable: Boolean
) {
    constructor(rs: ResultSet): this(
        name = rs.getString("COLUMN_NAME").lowercase(),
        type = rs.getInt("DATA_TYPE"),
        isNullable = rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable,
    )
}

fun <T> ResultSet.map(mapper: (ResultSet) -> T): Sequence<T> = sequence {
    while (next()) {
        yield(mapper(this@map))
    }
}

fun DatabaseMetaData.getColumnsInfo(table: String, schema: String? = null): List<ColumnInfo> =
    getColumns(null, schema, table, null).use { it.map(::ColumnInfo).toList() }

private val metaCache = mutableMapOf<String, List<ColumnInfo>>()

fun Connection.getTableMetaData(tableName: String, schema: String? = null): List<ColumnInfo> {
    val cacheKey = (schema.takeIf { !it.isNullOrBlank() }?.let { "$it." } ?: "") + tableName
    metaCache[cacheKey]?.let { return it }
    val result = metaData.getColumnsInfo(tableName, schema)
    metaCache[cacheKey] = result
    return result
}

private inline fun <T> Connection.withTransaction(block: () -> T): T {
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

private fun PreparedStatement.setParameter(index: Int, col: ColumnInfo, value: String?) {
    when {
        value.isNullOrEmpty() && col.isNullable -> setNull(index, col.type)
        value.isNullOrEmpty() -> throw IllegalStateException("NOT NULL column '${col.name}' is empty")
        else -> setString(index, value)
    }
}

fun Connection.upsert(
    items: List<Map<String, String>>,
    uniqueColumns: Set<String>,
    table: String,
    schema: String? = null
) {
    if (items.isEmpty()) return

    if (items.size > 1_000_000) {
        throw IllegalArgumentException("Items batch is too large: ${items.size}")
    }

    val columns = getTableMetaData(table, schema)

    if (columns.isEmpty()) {
        throw IllegalArgumentException("Table $table not found or has no columns")
    }

    val columnNames = columns.map { it.name.lowercase() }.toSet()

    if (!uniqueColumns.all { it.lowercase() in columnNames }) {
        val missingColumns = uniqueColumns.filterNot { it.lowercase() in columnNames }
        throw IllegalArgumentException(
            "Unique columns not found in table $table: ${missingColumns.joinToString()}"
        )
    }

    val itemsNormalized = items.map { item ->
        item.entries.associate { (k, v) -> k.lowercase() to v }
    }

    val allColumns = columns.map { it.name }

    fun quoteIdent(name: String) = "\"${name.replace("\"", "\"\"")}\""
    val quotedTable = if (schema != null) "${quoteIdent(schema)}.${quoteIdent(table)}" else quoteIdent(table)
    val quotedColumns = allColumns.map { quoteIdent(it) }

    val placeholdersForRow = "(" + columns.joinToString(", ") { "?" } + ")"
    val valuesPlaceholders = List(items.size) { placeholdersForRow }.joinToString(", ")

    val updateSet = allColumns
        .filterNot { col -> uniqueColumns.any { it.lowercase() == col.lowercase() } }
        .joinToString(", ") { "${quoteIdent(it)} = EXCLUDED.${quoteIdent(it)}" }

    val conflictTarget = uniqueColumns.joinToString(", ") { quoteIdent(it) }

    val sql = buildString {
        append("INSERT INTO $quotedTable (")
        append(quotedColumns.joinToString(", "))
        append(") VALUES ")
        append(valuesPlaceholders)
        append(" ON CONFLICT ($conflictTarget) ")
        if (updateSet.isNotEmpty()) {
            append("DO UPDATE SET $updateSet")
        } else {
            append("DO NOTHING")
        }
    }

    prepareStatement(sql).use { stmt ->
        withTransaction {
            itemsNormalized.withIndex().forEach { (itemIndex, item) ->
                columns.withIndex().forEach { (colIndex, col) ->
                    stmt.setParameter(
                        index = itemIndex * columns.size + colIndex + 1,
                        col = col,
                        value = item[col.name.lowercase()]
                    )
                }
            }
            stmt.executeUpdate()
        }
    }

    prepareStatement(sql).use { stmt ->
        val prevAutoCommit = autoCommit
        autoCommit = false
        try {
            for ((itemIndex, item) in itemsNormalized.withIndex()) {
                for ((colIndex, col) in columns.withIndex()) {
                    // PreparedStatement параметры нумеруются с 1
                    val paramIndex = itemIndex * columns.size + colIndex + 1
                    val value = item[col.name.lowercase()]

                    if (value.isNullOrEmpty()) {
                        if (col.isNullable) {
                            stmt.setNull(paramIndex, col.type)
                        } else {
                            throw IllegalStateException("NOT NULL column '${col.name}' is empty")
                        }
                    } else {
                        stmt.setString(paramIndex, value)
                    }
                }
            }

            stmt.executeUpdate()
            commit()
        } catch (e: Exception) {
            rollback()
            throw e
        } finally {
            autoCommit = prevAutoCommit
        }
    }
}

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
