# XML to PostgreSQL ETL

Kotlin CLI инструмент для эффективного ETL процесса из XML файлов в базу данных PostgreSQL с поддержкой параллельной обработки и автоматизации.

## 🚀 Возможности

- **Высокая производительность**: Stream-обработка XML, пул соединений, корутины для параллельной обработки
- **Универсальная распаковка**: Поддержка ZIP, TAR.GZ, TAR.BZ2, 7Z и других архивов
- **Автоопределение кодировок**: Автоматическое определение кодировки XML файлов (включая кириллицу)
- **Безопасная работа с БД**: Параметризованные запросы, транзакции, upsert операции
- **Гибкая конфигурация**: JSON файлы маппингов, .env файлы
- **Мониторинг**: Логирование прогресса и ошибок

## 📋 Требования

- Java 11+
- Kotlin
- PostgreSQL сервер
- Gradle 6.8+

## 🛠️ Установка и сборка

### Сборка проекта

```bash
# Клонирование репозитория
git clone <repository-url>
cd xml-to-pg-etl

# Сборка JAR файла
./gradlew build shadowJar

# Запуск сборки
java -jar build/libs/xml-to-pg-etl-1.0-all.jar --help
```

### Зависимости

Проект использует следующие основные библиотеки:
- kotlinx-cli для обработки аргументов командной строки
- HikariCP для pool-а соединений с БД
- Jackson для парсинга JSON конфигурации
- StAX для потоковой обработки XML
- Apache Commons Compress для распаковки архивов

## ⚙️ Конфигурация

### .env файл

Создайте `.env` файл с настройками подключения к базе данных:

```env
# Настройки подключения к PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_USER=username
DB_PASSWORD=password
DB_DATABASE=database_name

# Путь к файлу маппингов
MAPPINGS_FILE=./mappings.json

# Флаги очистки (опционально)
REMOVE_ARCHIVES_AFTER_UNPACK=false
REMOVE_XML_AFTER_IMPORT=false
```

### Файл маппингов (mappings.json)

```json
[
  {
    "xmlFile": ".*\\.xml$",
    "xmlTag": "item",
    "table": "products",
    "schema": "public",
    "uniqueColumns": ["id"],
    "batchSize": 1000,
    "attributes": {
      "name": "product_name",
      "price": "price",
      "category": "category_id"
    }
  },
  {
    "xmlFile": "users\\.xml",
    "xmlTag": "user",
    "table": "users",
    "schema": "auth",
    "uniqueColumns": ["email"],
    "batchSize": 500,
    "attributes": {
      "name": "full_name",
      "email": "email_address",
      "created_at": "registration_date"
    }
  }
]
```

## 📖 Использование

### Базовый синтаксис

```bash
java -jar <jar-file> --env <env-file> --xml <xml-source> [--extract-dir <directory>]
```

### Параметры командной строки

- `--env`, `-e`: **(обязательный)** Путь к .env файлу с конфигурацией
- `--xml`, `-x`: **(обязательный)** Путь к XML файлам, директории или архиву
- `--extract-dir`, `-d`: Директория для распаковки архивов (если не указана, используется временная)

### Примеры использования

#### 1. Обработка отдельных XML файлов

```bash
# Обработка одного XML файла
java -jar app.jar --env .env --xml ./data.xml

# Обработка всех XML файлов в директории
java -jar app.jar --env .env --xml ./xml_files/
```

#### 2. Обработка архивов

```bash
# Распаковка в временную директорию
java -jar app.jar --env .env --xml ./data.zip

# Распаковка в указанную директорию
java -jar app.jar --env .env --xml ./archive.tar.gz --extract-dir ./extracted
```

#### 3. Флаги очистки

```bash
# Удаление архивов после распаковки
REMOVE_ARCHIVES_AFTER_UNPACK=true java -jar app.jar --env .env --xml ./data.zip

# Удаление XML файлов после импорта
REMOVE_XML_AFTER_IMPORT=true java -jar app.jar --env .env --xml ./data.xml
```

## 📁 Структура проекта

```
src/main/kotlin/
├── Main.kt              # Главный класс приложения
├── config.kt            # Загрузка конфигурации
├── MappingTable.kt     # Конфигурация маппингов
├── PostgresUpserter.kt # Вставка данных в PostgreSQL
├── db.kt               # Утилиты для работы с БД
├── xml.kt              # XML парсер
├── archive.kt          # Распаковка архивов
└── ...
```

## 🔧 Поддерживаемые форматы

### XML форматы
- Файлы XML с различными кодировками (UTF-8, UTF-16, CP1251, KOI8-R)
- Включая кириллические символы
- Поддержка BOM и XML declaration

### Архивы
- ZIP (.zip)
- TAR (.tar)
- TAR.GZ (.tar.gz, .tgz)
- TAR.BZ2 (.tar.bz2, .tbz2)
- 7Z (.7z)
- GZIP (.gz)
- BZIP2 (.bz2)

### Типы данных PostgreSQL
Все основные типы:
- Строки: VARCHAR, TEXT, CHAR
- Числа: INT, BIGINT, DECIMAL, FLOAT, DOUBLE
- Даты: DATE, TIME, TIMESTAMP
- Логические: BOOLEAN
- Бинарные: BYTES, BLOB

## 📊 Производительность

- **Параллельная обработка**: До 4 корутин одновременно (конфигурируемо)
- **Batch вставки**: Настраиваемый размер пакетов (batchSize)
- **Connection pooling**: Автоматическая оптимизация пула соединений
- **Stream обработка**: Низкое потребление памяти при больших файлах

## 🚨 Ограничения

- Максимальный размер файла архива: 1GB
- Максимальный размер пакета для вставки: 1,000,000 записей
- PostgreSQL ограничения на длину имен таблиц и колонок (63 символа)

## 🔍 Логирование

Приложение использует стандартное логирование Java. Уровни логирования:
- **INFO**: Основная информация о прогрессе
- **WARNING**: Предупреждения и пропущенные файлы
- **SEVERE**: Ошибки, прерывающие обработку

Для настройки логирования можно использовать системные свойства Java:

```bash
java -Djava.util.logging.config.file=logging.properties -jar app.jar --env .env --xml data.xml
```
