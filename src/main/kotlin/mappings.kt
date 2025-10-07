package ru.my

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.io.File
import javax.sql.DataSource

data class MappingConfig(
    val db: Map<TableIdentifier, DatabaseMapping> = emptyMap(),
    val xml: XmlMapping,
) {
    private val xmlColumns by lazy {
        val keys = xml.values.filter { !it.value.notForSave }.keys

        when (db.keys.size) {
            1 -> {
                val defaultTable = db.keys.first()
                keys.map { key ->
                    if ('.' in key) ColumnIdentifier.parse(key)
                    else ColumnIdentifier(defaultTable, key)
                }
            }
            else -> {
                keys.map { key ->
                    if ('.' !in key) {
                        throw IllegalArgumentException(
                            "Short column name '$key' is ambiguous because multiple tables are defined. " +
                                    "Please use the full 'schema.table.column' format."
                        )
                    }
                    ColumnIdentifier.parse(key)
                }
            }
        }
    }

    init {
        require(db.isNotEmpty()) { "db section cannot be empty" }

        val referencedTables = xmlColumns.map { it.table }.toSet()
        val undefinedTables = referencedTables - db.keys
        if (undefinedTables.isNotEmpty()) {
            throw IllegalArgumentException(
                "Tables referenced in xml.values but not defined in db: $undefinedTables"
            )
        }
    }

    companion object {
        fun parseItems(file: File): List<MappingConfig> {
            val mapper = jacksonObjectMapper().apply {
                configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                registerModule(SimpleModule().apply {
                    addKeyDeserializer(TableIdentifier::class.java, TableIdentifierKeyDeserializer())
                })
            }
            return mapper.readValue(file)
        }
    }

    fun iteratePostgresUpserters(db: DataSource): Sequence<PostgresUpserter> = sequence {
        val columnsGroups = xmlColumns.groupBy { it.table }
            .map { (table, cols) -> table to cols.map { it.name }.toSet() }

        for ((table, columns) in columnsGroups) {
            val mapping = this@MappingConfig.db[table]
                ?: throw IllegalStateException("Table $table not found in db configuration. This should not happen.")
            val upserter = PostgresUpserter(db, table, columns, mapping.uniqueColumns)
            yield(upserter)
        }
    }
}

data class DatabaseMapping(
    @param:JsonProperty("unique_columns")
    val uniqueColumns: Set<String> = emptySet(),

    @param:JsonProperty("batch_size")
    val batchSize: Int = 500,
) {
    init {
        require(batchSize > 0) { "batchSize must be greater than 0, got $batchSize" }
    }
}

data class XmlMapping(
    val files: Set<String>,

    @param:JsonProperty("root_path")
    val rootPath: List<String>,

    val values: Map<String, ValueMapping>,

    val enums: Map<String, Set<String>> = emptyMap(),
) {
    val filesRegex by lazy { files.map { it.toRegex() } }

    init {
        require(files.isNotEmpty()) { "files set cannot be empty" }
        require(files.none { it.isBlank() }) { "files set cannot contain blank strings" }
        require(rootPath.isNotEmpty()) { "rootPath cannot be empty" }
        require(values.isNotEmpty()) { "values map cannot be empty" }
    }
}

data class ValueMapping(
    val path: List<String>,

    @param:JsonProperty("type")
    val valueType: XmlValueType = XmlValueType.ATTRIBUTE,

    val required: Boolean = false,

    @param:JsonProperty("not_for_save")
    val notForSave: Boolean = false,
) {
    init {
        require(path.isNotEmpty()) { "path list cannot be empty" }
    }

    fun toXmlValueConfig(outputKey: String): XmlValueConfig =
        XmlValueConfig(path, valueType, required, notForSave, outputKey)
}

data class ColumnIdentifier(val table: TableIdentifier, val name: String) {
    init {
        require(name.isNotBlank()) { "Column name must not be blank" }
    }

    companion object {
        fun parse(str: String): ColumnIdentifier {
            val parts = str.split('.')
            require(parts.size >= 2) { "'$str' is not a valid full column identifier (expected format: [schema.]table.column)" }

            return if (parts.size == 2) {
                ColumnIdentifier(
                    table = TableIdentifier(name = parts[0]),
                    name = parts[1]
                )
            } else {
                ColumnIdentifier(
                    table = TableIdentifier(name = parts[1], schema = parts[0]),
                    name = parts[2]
                )
            }
        }
    }
}
