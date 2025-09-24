package ru.my

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.io.File

data class MappingConfig(
    val db: DatabaseMapping,
    val xml: XmlMapping,
) {
    companion object {
        fun parseItems(file: File): List<MappingConfig> {
            val mapper = jacksonObjectMapper().apply {
                configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            }

            return mapper.readValue(file)
        }
    }
}

data class DatabaseMapping(
    val table: String,

    val schema: String? = null,

    @JsonProperty("unique_columns")
    val uniqueColumns: Set<String> = emptySet(),

    @JsonProperty("batch_size")
    val batchSize: Int = 500,
) {
    init {
        if (table.isBlank()) {
            throw IllegalArgumentException("table name is blank")
        }

        if (table.length > 63) {
            throw IllegalArgumentException("Table name too long (max 63 characters)")
        }

        if (batchSize <= 0) {
            throw IllegalArgumentException("batchSize must be greater than 0")
        }
    }
}

data class XmlMapping(
    val files: Set<String>,

    @JsonProperty("root_path")
    val rootPath: List<String>,

    val values: Map<String, ValueMapping>, // tagName : value

    val enums: Map<String, Set<String>> = emptyMap(), // tagName : values
) {
    val filesRegex = files.map { it.toRegex() }

    init {
        if (files.isEmpty()) {
            throw IllegalArgumentException("files is empty")
        }

        if (files.any { it.isBlank() }) {
            throw IllegalArgumentException("files contains blank")
        }

        if (rootPath.isEmpty()) {
            throw IllegalArgumentException("rootPath is empty")
        }

        if (values.isEmpty()) {
            throw IllegalArgumentException("values is empty")
        }
    }
}

data class ValueMapping(
    val path: List<String>,

    @JsonProperty("type")
    val valueType: XmlValueType = XmlValueType.ATTRIBUTE,

    val required: Boolean = false,
) {
    init {
        if (path.isEmpty()) {
            throw IllegalArgumentException("path is empty")
        }
    }
}
