package ru.my

import kotlinx.coroutines.delay
import java.sql.SQLException

suspend fun <T> retryOnDbError(
    times: Int = 3,
    initialDelay: Long = 100,
    block: suspend () -> T
): T {
    var lastException: Exception? = null
    var delay = initialDelay

    repeat(times) { attempt ->
        try {
            return block()
        } catch (e: SQLException) {
            lastException = e

            // Проверяем, стоит ли повторять
            when (e.sqlState) {
                "40001", // serialization_failure
                "40P01", // deadlock_detected
                "08000", // connection_exception
                "08006"  // connection_failure
                    -> {
                    if (attempt < times - 1) {
                        delay(delay)
                        delay *= 2 // экспоненциальная задержка
                    }
                }
                else -> throw e // не повторяем для других ошибок
            }
        }
    }

    throw lastException!!
}

//// Использование:
//retryOnDbError {
//    conn.upsert(batch, ...)
//}
