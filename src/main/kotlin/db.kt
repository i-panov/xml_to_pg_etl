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

inline fun <T> Connection.withTransaction(block: () -> T): T {
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

fun PreparedStatement.setParameter(index: Int, col: ColumnInfo, value: String?) {
    if (value.isNullOrEmpty()) {
        if (col.isNullable) {
            return setNull(index, col.type)
        } else {
            throw IllegalStateException("NOT NULL column '${col.name}' is empty")
        }
    }

    return setString(index, value)
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
