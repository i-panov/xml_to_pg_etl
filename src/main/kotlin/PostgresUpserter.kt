package ru.my

class PostgresUpserter(
    val table: String,
    val schema: String? = null,
    val uniqueColumns: Set<String>,
) {
    val quotedTable = quoteIdent(table.lowercase())
    val quotedScheme = if (schema != null) quoteIdent(schema.lowercase()) else null
    val fullyQualifiedTarget = if (quotedScheme != null) "${quotedScheme}.${quotedTable}" else quotedTable
    val quotedUniqueColumns = uniqueColumns.map { quoteIdent(it.lowercase()) }

    private fun quoteIdent(name: String) = "\"${name.replace("\"", "\"\"")}\""
}
