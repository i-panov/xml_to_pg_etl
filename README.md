# XML to PostgreSQL ETL

Высокопроизводительная Kotlin CLI утилита для загрузки данных из XML файлов в PostgreSQL с поддержкой сложных маппингов, параллельной обработки и автоматической распаковки архивов.

## 🎯 Ключевые возможности

- **🚄 Потоковая обработка**: Event-based XML парсинг без загрузки всего файла в память
- **⚡ Параллелизм**: Корутины для одновременной обработки XML → запись в несколько таблиц
- **📦 Умная распаковка**: Автоматическое определение и извлечение из ZIP, TAR.GZ, TAR.BZ2, 7Z и других архивов
- **🌐 Кодировки**: Автоопределение через BOM и XML declaration (UTF-8, UTF-16, Windows-1251, KOI8-R)
- **🎭 Гибкие маппинги**:
    - Извлечение данных из имён файлов через regex-группы
    - JSON объекты и массивы прямо из XML
    - Маппинг одного XML на несколько таблиц
    - Enum-валидация значений
- **💾 UPSERT по умолчанию**: `ON CONFLICT DO UPDATE` для идемпотентных загрузок
- **🔒 Транзакционность**: Batch-вставки с автоматическими retry при сетевых сбоях
- **📊 Мониторинг**: Детальное логирование прогресса и ошибок

## 📋 Требования

- **Java**: 21+ (для полной поддержки virtual threads и современных API)
- **PostgreSQL**: 11+ (для `ON CONFLICT` и JSON/JSONB)
- **Gradle**: 8.5+ (для Kotlin 2.x)

## 🛠️ Установка

### Сборка из исходников

```bash
git clone <repository-url>
cd xml-to-pg-etl

# Сборка fat JAR
./gradlew shadowJar

# Результат в build/libs/xml_to_pg_etl.jar
```

### Зависимости

```kotlin
// Основные
kotlinx-coroutines-core 1.10.2    // Параллелизм
HikariCP 7.0.2                    // Connection pooling
postgresql 42.7.5                 // JDBC драйвер
commons-compress 1.28.0           // Архивы
jackson 2.19.2                    // JSON конфиг
clikt 5.0.1                       // CLI
ktoml 0.7.1                       // TOML конфиг
logback 1.5.18                    // Логирование
```

## ⚙️ Конфигурация

### 1. Конфигурационный файл (TOML)

Создайте `config.toml`:

```toml
# Путь к маппингам
mappingsFile = "./mappings.json"

# Опции очистки
removeArchivesAfterUnpack = false
removeXmlAfterImport = false

# Остановка при первой ошибке
stopOnError = false

# Максимальный размер файла в архиве (байты)
maxArchiveItemSize = 1073741824  # 1 GB

[db]
host = "localhost"
port = 5432
user = "etl_user"
password = "secure_password"
database = "analytics"

[db.props]
connectionTimeout = 30      # секунды
idleTimeout = 120          # секунды
maxLifetime = 1200         # секунды (20 минут)
validationTimeout = 5      # секунды
socketTimeout = 900        # секунды (15 минут)
schema = "public"          # схема по умолчанию
appName = "XML_ETL"        # имя приложения в pg_stat_activity
```

### 2. Файл маппингов (JSON)

**Простой пример** (одна таблица):

```json
[
  {
    "xml": {
      "files": ["products_\\d{8}\\.xml"],
      "rootPath": ["catalog", "product"],
      "values": {
        "id": {
          "path": ["id"],
          "type": "ATTRIBUTE",
          "required": true
        },
        "name": {
          "path": ["name"],
          "type": "CONTENT"
        },
        "price": {
          "path": ["price"],
          "type": "CONTENT"
        }
      }
    },
    "db": {
      "products": {
        "unique_columns": ["id"],
        "batch_size": 1000
      }
    }
  }
]
```

**Сложный пример** (несколько таблиц + JSON + валидация):

```json
[
  {
    "xml": {
      "files": ["orders_(?<date>\\d{8})_(?<region>\\w+)\\.xml"],
      "rootPath": ["orders", "order"],
      "values": {
        "shop.order_id": {
          "path": ["id"],
          "type": "ATTRIBUTE",
          "required": true
        },
        "shop.customer_name": {
          "path": ["customer", "name"],
          "type": "CONTENT"
        },
        "shop.status": {
          "path": ["status"],
          "type": "ATTRIBUTE",
          "required": true
        },
        "items.order_id": {
          "path": ["id"],
          "type": "ATTRIBUTE"
        },
        "items.products": {
          "path": ["items", "item"],
          "type": "JSON_ARRAY",
          "structure": {
            "sku": {
              "path": ["sku"],
              "type": "ATTRIBUTE"
            },
            "quantity": {
              "path": ["qty"],
              "type": "CONTENT"
            },
            "price": {
              "path": ["price"],
              "type": "CONTENT"
            }
          }
        }
      },
      "enums": {
        "shop.status": ["new", "processing", "shipped", "delivered"]
      }
    },
    "db": {
      "shop.orders": {
        "unique_columns": ["order_id"],
        "batch_size": 500,
        "filename_groups_to_columns": {
          "date": "order_date",
          "region": "region_code"
        }
      },
      "shop.order_items": {
        "unique_columns": ["order_id"],
        "batch_size": 1000
      }
    }
  }
]
```

### 3. Структура маппинга

#### XML секция

| Поле | Тип | Описание |
|------|-----|----------|
| `files` | `string[]` | Regex-паттерны имён файлов. Можно использовать именованные группы `(?<name>...)` |
| `rootPath` | `string[]` | Путь от корня XML до повторяющегося элемента-записи |
| `values` | `object` | Маппинг XML → колонки. Ключ: `table.column` или просто `column` |
| `enums` | `object` | Словарь допустимых значений для валидации |

#### Типы значений (`type`)

| Тип | Описание | Пример |
|-----|----------|--------|
| `ATTRIBUTE` | Атрибут элемента | `<item id="123">` → `id` |
| `CONTENT` | Текстовое содержимое | `<name>Product</name>` → `Product` |
| `JSON_OBJECT` | Объект JSON из вложенных элементов | `{"field": "value"}` |
| `JSON_ARRAY` | Массив JSON из повторяющихся элементов | `[{"id": 1}, {"id": 2}]` |

#### Дополнительные параметры значений

```json
{
  "table.column": {
    "path": ["path", "to", "element"],
    "type": "CONTENT",
    "required": true,          // Пропустить запись, если нет значения
    "not_for_save": false,     // Использовать только для валидации
    "default_value": "false",  // Значение по умолчанию, если поле отсутствует в XML
    "structure": {...}         // Для JSON_OBJECT/JSON_ARRAY
  }
}
```

**Параметр `default_value`**:

Используется когда значение отсутствует в XML и столбец имеет ограничение `NOT NULL` в PostgreSQL.

```json
{
  "is_active": {
    "path": ["isActive"],
    "type": "ATTRIBUTE",
    "default_value": "false"
  }
}
```

**Приоритет обработки `null` значений**:

1. Если значение есть в XML → используется оно
2. Если значения нет, но задан `default_value` в маппинге → используется `default_value`
3. Если значения нет и `default_value` не задан, но в PostgreSQL есть `DEFAULT` → используется значение из DDL таблицы
4. Если ничего нет и столбец `NOT NULL` → ошибка `IllegalStateException`

**Пример**:

```sql
-- Таблица с NOT NULL DEFAULT
CREATE TABLE products (
    id INT PRIMARY KEY,
    is_active BOOLEAN NOT NULL DEFAULT false
);
```

```json
{
  "is_active": {
    "path": ["isActive"],
    "type": "ATTRIBUTE"
    // default_value не указан — будет использован DEFAULT false из DDL
  }
}
```

#### DB секция

| Поле | Тип | Описание |
|------|-----|----------|
| `unique_columns` | `string[]` | Колонки для `ON CONFLICT` |
| `batch_size` | `int` | Размер пакета для batch insert (по умолчанию 500) |
| `filename_groups_to_columns` | `object` | Маппинг regex-групп из имени файла → колонки |

## 📖 Использование

### Синтаксис

```bash
java -jar xml_to_pg_etl.jar \
  --env config.toml \
  --xml <источник> \
  [--extract-dir <директория>]
```

### Параметры

| Флаг | Короткий | Обязательный | Описание |
|------|----------|--------------|----------|
| `--env` | `-e` | ✅ | Путь к TOML конфигу |
| `--xml` | `-x` | ✅ | XML файл, директория или архив |
| `--extract-dir` | `-d` | ❌ | Директория для распаковки (иначе temp) |

### Примеры

#### 1. Один XML файл

```bash
java -jar xml_to_pg_etl.jar -e config.toml -x data.xml
```

#### 2. Директория с XML

```bash
java -jar xml_to_pg_etl.jar -e config.toml -x /data/xml_files/
```

Обработает все `*.xml` + автоматически распакует найденные архивы.

#### 3. Архив

```bash
# Во временную директорию
java -jar xml_to_pg_etl.jar -e config.toml -x export.zip

# В указанную директорию
java -jar xml_to_pg_etl.jar -e config.toml -x export.tar.gz -d /tmp/extracted
```

#### 4. С очисткой

```bash
# Изменить конфиг:
removeArchivesAfterUnpack = true
removeXmlAfterImport = true

java -jar xml_to_pg_etl.jar -e config.toml -x data.zip
```

#### 5. Остановка при ошибке

```bash
# В конфиге:
stopOnError = true

java -jar xml_to_pg_etl.jar -e config.toml -x data.xml
```

По умолчанию утилита логирует ошибки, но продолжает обработку.

## 🏗️ Архитектура

### Структура проекта

```
src/main/kotlin/ru/my/
├── Main.kt                      # Entry point + pipeline оркестрация
├── config.kt                    # Парсинг TOML, настройка HikariCP
├── mappings.kt                  # Модель маппингов + валидация
│
├── archive.kt                   # Распаковка архивов
├── streams.kt                   # Streaming copy с валидацией размера
│
├── xml/
│   ├── xml_events.kt           # StAX парсер → XmlEvent sequence
│   ├── xml_nodes.kt            # Преобразование событий → XmlNode дерево
│   ├── xml_filters.kt          # Извлечение данных по маппингам
│   └── xml_utils.kt            # Детекция кодировки (BOM + XML decl)
│
└── db/
    ├── db.kt                   # Расширения для JDBC + type mapping
    └── PostgresUpserter.kt     # Генерация UPSERT SQL + batch execute
```

### Поток обработки

```
Источник → Извлечение → Парсинг → Маппинг → Батчинг → Upsert
   ↓           ↓           ↓          ↓         ↓        ↓
Archive    Flow<Path>  Sequence   Channel   List    PostgreSQL
  .zip      xml files   <XmlNode>  buffered  <Map>   transaction
```

1. **Извлечение**: `Flow<Path>` из архива/директории
2. **Парсинг**: StAX → `Sequence<XmlEvent>` → `Sequence<XmlNode>`
3. **Маппинг**: Применение `XmlValueConfig` → `Map<String, String>`
4. **Распределение**: Producer отправляет в каналы для каждой таблицы
5. **Батчинг**: Consumers собирают батчи и пишут в БД
6. **Upsert**: `INSERT ... ON CONFLICT (unique_cols) DO UPDATE`

### Параллелизм

```kotlin
// Main.kt
val concurrency = Runtime.getRuntime().availableProcessors()

xmlFiles
  .flatMapMerge(concurrency) { xmlFile ->
    // Каждый файл обрабатывается параллельно
    flow { 
      processXmlToMultipleTables(xmlFile, db)
    }
  }

// Внутри processXmlToMultipleTables:
// - 1 producer корутина (парсинг XML)
// - N consumer корутин (по одной на таблицу)
```

## 🔧 Поддерживаемые форматы

### Архивы

| Формат | Расширения | Примечание |
|--------|------------|------------|
| ZIP | `.zip` | Deflate, Store |
| TAR | `.tar` | |
| TAR.GZ | `.tar.gz`, `.tgz` | |
| TAR.BZ2 | `.tar.bz2`, `.tbz2` | |
| GZIP | `.gz` | Одиночные файлы |
| BZIP2 | `.bz2` | Одиночные файлы |
| 7-Zip | `.7z` | |

**Важно**: Вложенные архивы не поддерживаются (архив внутри архива).

### Кодировки XML

Автоопределение через:
1. **BOM** (Byte Order Mark): UTF-8, UTF-16BE/LE, UTF-32BE/LE
2. **XML declaration**: `<?xml version="1.0" encoding="windows-1251"?>`
3. **По умолчанию**: UTF-8

Поддержка: все кодировки из `java.nio.charset.Charset`.

### Типы данных PostgreSQL

| SQL тип | Kotlin тип | Примечание |
|---------|------------|------------|
| `VARCHAR`, `TEXT` | `String` | |
| `INTEGER` | `Int` | |
| `BIGINT` | `Long` | |
| `DECIMAL`, `NUMERIC` | `BigDecimal` | Запятая → точка |
| `FLOAT`, `DOUBLE` | `Double` | Запятая → точка |
| `BOOLEAN` | `Boolean` | `true/false/1/0/yes/no` |
| `DATE` | `LocalDate` | `YYYY-MM-DD` |
| `TIME` | `LocalTime` | `HH:mm:ss` |
| `TIMESTAMP` | `LocalDateTime` | ISO-8601 |
| `TIMESTAMPTZ` | `OffsetDateTime` | ISO-8601 с timezone |
| `BYTEA` | `ByteArray` | Hex string → bytes |
| `JSON`, `JSONB` | `String` | Передаётся как текст |

**Обработка отсутствующих значений**:

- Если значение отсутствует в XML и колонка `NULLABLE` → вставляется `NULL`
- Если значение отсутствует и задан `default_value` в маппинге → используется `default_value`
- Если значение отсутствует, `default_value` не задан, но в PostgreSQL есть `DEFAULT` → используется значение из DDL
- Если значение отсутствует, нет `default_value`, и колонка `NOT NULL` → ошибка

## ⚡ Производительность

### Бенчмарки

| Сценарий | Производительность | Настройки |
|----------|-------------------|-----------|
| 1M записей (10 колонок) | ~60 сек | `batchSize=1000`, 8 cores |
| Архив 500MB (XML внутри) | ~120 сек | Включая распаковку |
| 10 XML → 5 таблиц | ~45 сек | Параллельная запись |

### Оптимизация

#### Connection Pool

```toml
# config.toml
[db.props]
connectionTimeout = 30
maxLifetime = 1200
socketTimeout = 900
```

Формула: `poolSize = min(cores * 2, 16)`, где cores = количество ядер.

#### Batch Size

```json
{
  "db": {
    "table_name": {
      "batch_size": 1000  // ↑ для широких таблиц, ↓ для узких
    }
  }
}
```

**Эмпирика**:
- 5-10 колонок → `batch_size: 1000-2000`
- 20+ колонок → `batch_size: 500`
- JSON колонки → `batch_size: 200-300`

#### Retry

```kotlin
// Main.kt (автоматически)
.retry(3) { e ->
  // Повтор при сетевых ошибках
  e is SQLException && 
    ("socket" in e.message || "timeout" in e.message)
}
```

## 🚨 Ограничения

| Параметр | Лимит | Обход |
|----------|-------|-------|
| Размер файла в архиве | 1 GB | `maxArchiveItemSize` в конфиге |
| Размер батча | 1,000,000 строк | Теоретический лимит памяти |
| Длина имени таблицы/колонки | 63 символа | Ограничение PostgreSQL |
| Глубина вложенности XML | Без ограничений | Ограничено памятью JVM |
| Параллельных задач | `cores * 2` | Зависит от connection pool |

## 📊 Логирование

### Уровни

```
INFO  → Старт/завершение файлов, количество записей
WARN  → Пропущенные файлы, невалидные записи
ERROR → Ошибки парсинга, SQL ошибки
DEBUG → Детали батчей (при duration > 60s)
```

### Пример вывода

```
INFO  Extracting archive: data.zip to /tmp/xml_to_pg_etl_12345
INFO  Detected encoding for export.xml: windows-1251
INFO  Starting processing export.xml -> orders. Query: INSERT INTO "orders" ...
INFO  Processed 50000 records (path: orders/order) from export.xml
INFO  Completed export.xml -> orders: 120 batches, 119847 rows
INFO  Successfully loaded export.xml: 119847 total rows across 2 tables
INFO  Processing complete: 5 files found, 5 processed successfully, 0 with errors
```

### Настройка

`logback.xml`:

```xml
<configuration>
  <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
    <encoder>
      <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
    </encoder>
  </appender>

  <!-- Детальный лог для вашего кода -->
  <logger name="ru.my" level="DEBUG"/>
  
  <!-- Меньше шума от библиотек -->
  <logger name="com.zaxxer.hikari" level="WARN"/>
  <logger name="org.apache.commons.compress" level="WARN"/>

  <root level="INFO">
    <appender-ref ref="STDOUT"/>
  </root>
</configuration>
```

## 🐛 Troubleshooting

### `OutOfMemoryError`

**Причина**: Слишком большие батчи или огромные JSON поля.

**Решение**:
```bash
java -Xmx4G -jar xml_to_pg_etl.jar ...  # Увеличить heap
```

Или уменьшить `batch_size` в маппинге.

### `Connection timeout`

**Причина**: Connection pool исчерпан.

**Решение**:
```toml
[db.props]
connectionTimeout = 60  # Увеличить таймаут
```

Или уменьшить параллелизм (меньше файлов одновременно).

### `Encoding issues (кракозябры)`

**Причина**: Кодировка не определена корректно.

**Решение**: Добавить `encoding` в XML declaration:
```xml
<?xml version="1.0" encoding="windows-1251"?>
```

### `No mapping found for file.xml`

**Причина**: Имя файла не соответствует regex в `files`.

**Решение**: Проверить паттерн:
```json
{
  "files": [".*\\.xml"]  // Слишком широко
  "files": ["data_\\d+\\.xml"]  // Правильный паттерн
}
```

### `Column not found in table`

**Причина**: Опечатка в имени колонки или колонка не существует.

**Решение**:
1. Проверить DDL таблицы: `\d table_name` в psql
2. Исправить маппинг

## 🔐 Безопасность

- ✅ **Параметризованные запросы**: Нет SQL injection
- ✅ **Транзакции**: Batch откатывается при ошибке
- ✅ **Credentials в конфиге**: Не хардкодить в коде!
- ⚠️ **Path traversal**: Будьте осторожны с архивами из ненадёжных источников

## 📝 Changelog

### v1.0 (текущая)

- ✨ Переход с `.env` на TOML конфигурацию
- ✨ Поддержка JSON объектов/массивов в маппинге
- ✨ Извлечение данных из имён файлов через regex
- ✨ Маппинг одного XML на несколько таблиц
- ✨ Enum-валидация значений
- ✨ Автоопределение кодировок через BOM
- 🐛 Исправлен memory leak в каналах
- ⚡ Оптимизация connection pool sizing
