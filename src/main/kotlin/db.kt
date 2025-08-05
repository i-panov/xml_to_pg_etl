package ru.my

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import java.sql.DatabaseMetaData

data class ColumnInfo(
    val name: String,
    val type: Int,
    val isNullable: Boolean
)

private val metaCache = mutableMapOf<String, List<ColumnInfo>>()

fun Connection.getTableMetaData(tableName: String, schema: String? = null): List<ColumnInfo> {
    val cacheKey = (schema.takeIf { !it.isNullOrBlank() }?.let { "$it." } ?: "") + tableName

    metaCache[cacheKey]?.let { return it }

    val result = mutableListOf<ColumnInfo>()

    metaData.getColumns(null, schema, tableName, null).use { rs ->
        while (rs.next()) {
            val columnName = rs.getString("COLUMN_NAME").lowercase()
            val columnType = rs.getInt("DATA_TYPE")
            val isNullable = rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable
            result.add(ColumnInfo(columnName, columnType, isNullable))
        }
    }

    metaCache[cacheKey] = result
    return result
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

    val columns = getTableMetaData(table, schema).also {
        if (it.isEmpty()) throw IllegalArgumentException("Table $table not found or has no columns")
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

    val placeholdersForRow = "(" + allColumns.joinToString(", ") { "?" } + ")"
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
        val prevAutoCommit = autoCommit
        autoCommit = false
        try {
            var paramIndex = 1
            for (item in itemsNormalized) {
                for (col in columns) {
                    val value = item[col.name.lowercase()]
                    if (value.isNullOrEmpty()) {
                        if (col.isNullable) {
                            stmt.setNull(paramIndex++, col.type)
                        } else {
                            throw IllegalStateException("NOT NULL column '${col.name}' is empty")
                        }
                    } else {
                        stmt.setString(paramIndex++, value)
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
