package ru.my

import javax.sql.DataSource

class PostgresUpserter(
    val dataSource: DataSource,
    val table: String,
    val schema: String? = null,
    val uniqueColumns: Set<String>,
) {
    private val quotedTable = quoteIdent(table.lowercase())
    private val quotedSchema = if (schema != null) quoteIdent(schema.lowercase()) else null
    private val fullyQualifiedTarget = if (quotedSchema != null) "${quotedSchema}.${quotedTable}" else quotedTable
    private val conflictTarget = uniqueColumns.joinToString(", ") { quoteIdent(it) }

    private val allTableColumns = run {
        dataSource.connection.use {
            it.metaData.getColumnsInfo(table, schema)
        }
    }

    init {
        if (allTableColumns.isEmpty()) {
            throw IllegalArgumentException("Table $table not found or has no columns")
        }

        val columnNames = allTableColumns.map { it.name.lowercase() }.toSet()

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

        val itemsNormalized = items.map { item ->
            item.entries.associate { (k, v) -> k.lowercase() to v }
        }

        // Находим пересечение колонок из БД и колонок из items
        val itemColumnNames = itemsNormalized.flatMap { it.keys }.toSet()
        val workingColumns = allTableColumns.filter { col ->
            col.name.lowercase() in itemColumnNames
        }

        if (workingColumns.isEmpty()) {
            throw IllegalArgumentException("No matching columns found between items and table schema")
        }

        val quotedWorkingColumns = workingColumns.map { quoteIdent(it.name) }
        val placeholdersForRow = "(" + workingColumns.joinToString(", ") { "?" } + ")"
        val valuesPlaceholders = List(items.size) { placeholdersForRow }.joinToString(", ")

        // Формируем SET часть только для не-уникальных колонок, которые есть в данных
        val updateSet = workingColumns
            .filterNot { col -> uniqueColumns.any { it.lowercase() == col.name.lowercase() } }
            .joinToString(", ") { "${quoteIdent(it.name)} = EXCLUDED.${quoteIdent(it.name)}" }

        val sql = buildString {
            append("INSERT INTO $fullyQualifiedTarget (")
            append(quotedWorkingColumns.joinToString(", "))
            append(") VALUES ")
            append(valuesPlaceholders)
            append(" ON CONFLICT ($conflictTarget) ")
            if (updateSet.isNotEmpty()) {
                append("DO UPDATE SET $updateSet")
            } else {
                append("DO NOTHING")
            }
        }

        dataSource.connection.use { conn ->
            conn.apply {
                prepareStatement(sql).use { stmt ->
                    withTransaction {
                        itemsNormalized.withIndex().forEach { (itemIndex, item) ->
                            workingColumns.withIndex().forEach { (colIndex, col) ->
                                stmt.setParameter(
                                    // PreparedStatement параметры нумеруются с 1
                                    index = itemIndex * workingColumns.size + colIndex + 1,
                                    col = col,
                                    value = item[col.name.lowercase()]
                                )
                            }
                        }
                        stmt.executeUpdate()
                    }
                }
            }
        }
    }

    private fun quoteIdent(name: String) = "\"${name.replace("\"", "\"\"")}\""
}
