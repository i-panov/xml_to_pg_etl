package ru.my.db

import javax.sql.DataSource

// TODO: В планах опционально добавить возможность вместо обычного UPSERT делать COPY в TEMP/UNLOGGED таблицу
// и вставку уже из нее в основную таблицу. Это может сильно улучшить производительность.
// Однако для этого понадобится вдвое больше памяти, поэтому эта возможность не будет использоваться по умолчанию.
// Можно будет указывать для каждой конкретной таблицы в маппинге.

class PostgresUpserter(
    val dataSource: DataSource,
    val table: TableIdentifier,
    val targetColumns: Set<String>,
    val uniqueColumns: Set<String>,
) {
    private val allTableColumns by lazy {
        dataSource.connection.use { it.metaData.getColumnsInfo(table) }
    }

    init {
        if (allTableColumns.isEmpty()) {
            throw IllegalArgumentException("Table $table not found or has no columns")
        }

        if (targetColumns.isEmpty()) {
            throw IllegalArgumentException("No target columns specified")
        }

        val columnNames = allTableColumns.map { it.name.lowercase() }.toSet()

        val validationMap = sequenceOf(
            "Target" to targetColumns,
            "Unique" to uniqueColumns,
        )

        for ((name, columns) in validationMap) {
            val missingColumns = columns.filterNot { it.lowercase() in columnNames }

            if (missingColumns.isNotEmpty()) {
                throw IllegalArgumentException(
                    "$name columns not found in table $table: ${missingColumns.joinToString()}"
                )
            }
        }
    }

    val workingColumns by lazy {
        allTableColumns.filter { col ->
            targetColumns.any { it.lowercase() == col.name.lowercase() }
        }
    }

    val sql by lazy {
        buildString {
            val quotedWorkingColumns = workingColumns.map { quoteDbIdent(it.name) }

            val placeholdersForRow = workingColumns.joinToString(", ") { "?" }

            val conflictTarget = uniqueColumns.joinToString(", ") { quoteDbIdent(it) }

            val updateSet = workingColumns
                .filterNot { col -> uniqueColumns.any { it.lowercase() == col.name.lowercase() } }
                .joinToString(", ") { "${quoteDbIdent(it.name)} = EXCLUDED.${quoteDbIdent(it.name)}" }

            append("INSERT INTO ${table.fullyQualifiedName} (")
            append(quotedWorkingColumns.joinToString(", "))
            append(") VALUES ($placeholdersForRow)")

            if (conflictTarget.isNotBlank()) {
                append(" ON CONFLICT ($conflictTarget) ")

                if (updateSet.isNotEmpty()) {
                    append("DO UPDATE SET $updateSet")
                } else {
                    append("DO NOTHING")
                }
            }
        }
    }

    fun execute(items: List<Map<String, String>>) {
        if (items.isEmpty()) {
            return
        }

        // Эта проверка больше не нужна, так как мы не ограничены количеством параметров,
        // но оставим ее как защиту от передачи абсурдно больших списков в память.
        if (items.size > 1_000_000) {
            throw IllegalArgumentException("Items batch is too large to process in memory: ${items.size}")
        }

        // Нормализуем ключи в нижний регистр для консистентности
        val itemsNormalized = items.map { item ->
            item.entries.associate { (k, v) -> k.lowercase() to v }
        }

        // Фильтруем дубликаты по уникальным колонкам
        val uniqueItems = filterDuplicates(itemsNormalized)

        if (uniqueItems.isEmpty()) {
            return
        }

        dataSource.connection.use { conn ->
            conn.withTransaction {
                conn.prepareStatement(sql).use { stmt ->
                    for (item in uniqueItems) {
                        workingColumns.withIndex().forEach { (colIndex, col) ->
                            stmt.setParameter(
                                index = colIndex + 1, // PreparedStatement параметры нумеруются с 1
                                col = col,
                                value = item[col.name.lowercase()]
                            )
                        }

                        stmt.addBatch()
                    }

                    stmt.executeBatch()
                }
            }
        }
    }

    private fun filterDuplicates(items: List<Map<String, String>>): List<Map<String, String>> {
        if (uniqueColumns.isEmpty()) return items

        val seen = mutableSetOf<List<String>>()
        val result = mutableListOf<Map<String, String>>()

        for (item in items) {
            val uniqueKey = uniqueColumns.map { col ->
                item[col.lowercase()] ?: throw IllegalStateException("Missing unique column $col in item")
            }

            if (seen.add(uniqueKey)) {
                result.add(item)
            }
        }

        println("Filtered ${items.size - result.size} duplicates from ${items.size} items")
        return result
    }
}
