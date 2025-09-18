plugins {
    kotlin("jvm") version "2.1.10"
    id("com.github.johnrengelman.shadow") version "7.1.2"
    application
}

group = "ru.my"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

application {
    mainClass = "ru.my.MainKt"
}

dependencies {
    testImplementation(kotlin("test"))

    // Корутины
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")

    // HikariCP
    implementation("com.zaxxer:HikariCP:5.0.1")

    // PostgreSQL
    implementation("org.postgresql:postgresql:42.7.4")

    // Jackson для XML
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-xml:2.19.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.19.0")

    // Kotlin reflect (та же версия, что и Kotlin)
    implementation(kotlin("reflect"))

    // Commons Compress
    implementation("org.apache.commons:commons-compress:1.26.1")

    // CLI
    implementation("org.jetbrains.kotlinx:kotlinx-cli:0.3.6")

    // Логгирование
    implementation("ch.qos.logback:logback-classic:1.4.14")
}

tasks.test {
    useJUnitPlatform()
}

tasks.shadowJar {
    archiveFileName.set("xml_to_pg_etl.jar")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    mergeServiceFiles()
    manifest {
        attributes["Main-Class"] = "ru.my.MainKt"
    }
}

kotlin {
    jvmToolchain(21)
}
