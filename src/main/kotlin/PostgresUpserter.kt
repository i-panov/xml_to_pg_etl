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

    private val columns = run {
        dataSource.connection.use {
            it.metaData.getColumnsInfo(table, schema)
        }
    }

    private val placeholdersForRow = "(" + columns.joinToString(", ") { "?" } + ")"

    private val allColumns = columns.map { it.name }
    private val quotedColumns = allColumns.map { quoteIdent(it) }

    private val updateSet = allColumns
        .filterNot { col -> uniqueColumns.any { it.lowercase() == col.lowercase() } }
        .joinToString(", ") { "${quoteIdent(it)} = EXCLUDED.${quoteIdent(it)}" }

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

    // TODO: возможно стоит при парсинге XML не преобразовывать все-таки значения в строку, а оставлять как есть
    // Теперь, когда в PreparedStatement.setParameter есть маппинг по типам, возможно это уменьшило бы количество
    // преобразований и сделало процесс быстрее. Задача на перспективу: проверить эту теорию.
    fun execute(items: List<Map<String, String>>) {
        if (items.isEmpty()) return

        if (items.size > 1_000_000) {
            throw IllegalArgumentException("Items batch is too large: ${items.size}")
        }

        // TODO: сравнивать схему из базы со списком столбцов в items и брать только те столбцы которые переданы в items

        val itemsNormalized = items.map { item ->
            item.entries.associate { (k, v) -> k.lowercase() to v }
        }

        val valuesPlaceholders = List(items.size) { placeholdersForRow }.joinToString(", ")

        val sql = buildString {
            append("INSERT INTO $fullyQualifiedTarget (")
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

        dataSource.connection.use { conn ->
            conn.apply {
                prepareStatement(sql).use { stmt ->
                    withTransaction {
                        itemsNormalized.withIndex().forEach { (itemIndex, item) ->
                            columns.withIndex().forEach { (colIndex, col) ->
                                stmt.setParameter(
                                    // PreparedStatement параметры нумеруются с 1
                                    index = itemIndex * columns.size + colIndex + 1,
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
