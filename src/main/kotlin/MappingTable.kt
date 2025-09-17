package ru.my

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.io.File

data class MappingTable(
    val xmlFile: String,
    val xmlTags: List<String>,
    val table: String,
    val schema: String?,
    val uniqueColumns: Set<String> = emptySet(),
    val batchSize: Int = 500,
    val attributes: Map<String, String> = emptyMap(), // xml : column
    val enumValues: Map<String, Set<String>> = emptyMap(), // tagName : values
) {
    val xmlFileRegex = Regex(xmlFile)

    fun validate(): Map<String, String> {
        val errors = mutableMapOf<String, String>()

        if (xmlFile.isBlank()) errors["xmlFile"] = "xmlFile is blank"
        if (xmlTags.isEmpty()) errors["xmlTags"] = "xmlTags is blank"

        if (table.isBlank()) errors["table"] = "table name is blank"
        if (table.length > 63) { // PostgreSQL limit
            errors["table.length"] = "Table name too long (max 63 characters)"
        }

        val validNamePattern = "^[a-z_][a-z0-9_$]*$".toRegex(RegexOption.IGNORE_CASE)
        if (!validNamePattern.matches(table)) {
            errors["table"] = "Invalid characters"
        }

        if (uniqueColumns.isEmpty()) errors["uniqueColumns"] = "must not be empty"
        if (batchSize <= 0) errors["batchSize"] = "must be greater than 0"
        if (attributes.isEmpty()) errors["attributes"] = "must not be empty"
        if (attributes.keys.any { it.isBlank() }) errors["attributes.xml"] = "some xml keys are blank"
        if (attributes.values.any { it.isBlank() }) errors["attributes.column"] = "some column names are blank"

        // Проверка на дублирующиеся колонки
        val duplicateColumns = attributes.values.groupingBy { it.lowercase() }.eachCount().filter { it.value > 1 }
        if (duplicateColumns.isNotEmpty()) {
            errors["attributes.duplicate"] = "duplicate column mappings: ${duplicateColumns.keys.joinToString()}"
        }

        // Проверка, что все уникальные колонки присутствуют в списке column'ов
        val mappedColumns = attributes.values.map { it.lowercase() }.toSet()
        val missingUniques = uniqueColumns.filterNot { it.lowercase() in mappedColumns }
        if (missingUniques.isNotEmpty()) {
            errors["uniqueColumns.mapping"] = "some uniqueColumns are not mapped in attributes: ${missingUniques.joinToString()}"
        }

        attributes.values.forEach { col ->
            if (col.length > 63) {
                errors["column.length"] = "Column name '$col' too long (max 63 characters)"
            }
        }

        val emptyEnumKeys = enumValues.mapNotNull { (k, v) -> if (v.isEmpty()) k else null }

        if (emptyEnumKeys.isNotEmpty()) {
            errors["enumValues.empty"] = "some enum keys have no values: ${emptyEnumKeys.joinToString()}"
        }

        return errors
    }

    companion object {
        fun parseItems(file: File): List<MappingTable> {
            val mapper = jacksonObjectMapper().apply {
                configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            }

            return mapper.readValue(file)
        }
    }
}
