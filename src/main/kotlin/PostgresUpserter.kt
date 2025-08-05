package ru.my

import javax.sql.DataSource

class PostgresUpserter(
    val dataSource: DataSource,
    val table: String,
    val schema: String? = null,
    val uniqueColumns: Set<String>,
) {
    val quotedTable = quoteIdent(table.lowercase())
    val quotedSchema = if (schema != null) quoteIdent(schema.lowercase()) else null
    val fullyQualifiedTarget = if (quotedSchema != null) "${quotedSchema}.${quotedTable}" else quotedTable
    val quotedUniqueColumns = uniqueColumns.map { quoteIdent(it.lowercase()) }

    val columns = run {
        dataSource.connection.use {
            it.metaData.getColumnsInfo(table, schema)
        }
    }

    val placeholdersForRow = "(" + columns.joinToString(", ") { "?" } + ")"

    init {
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
    }

    fun execute(items: List<Map<String, String>>) {
        if (items.isEmpty()) return

        if (items.size > 1_000_000) {
            throw IllegalArgumentException("Items batch is too large: ${items.size}")
        }

        dataSource.connection.use { conn ->
            conn.apply {
                autoCommit = false
            }
        }
    }

    private fun quoteIdent(name: String) = "\"${name.replace("\"", "\"\"")}\""
}
