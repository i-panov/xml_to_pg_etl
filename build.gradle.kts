plugins {
    kotlin("jvm") version "2.2.20"
    id("com.github.johnrengelman.shadow") version "8.1.1"
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
    implementation(kotlin("stdlib"))
    testImplementation(kotlin("test"))

    // Корутины
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")

    // HikariCP
    implementation("com.zaxxer:HikariCP:7.0.2")

    // PostgreSQL
    implementation("org.postgresql:postgresql:42.7.5")

    // Jackson для XML
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-xml:2.19.2")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.19.2")

    // Kotlin reflect (та же версия, что и Kotlin)
    implementation(kotlin("reflect"))

    // Commons Compress
    implementation("org.apache.commons:commons-compress:1.28.0")

    // CLI
    implementation("com.github.ajalt.clikt:clikt:5.0.1")

    // Логгирование
    implementation("ch.qos.logback:logback-classic:1.5.18")

    // TOML
    implementation("com.akuleshov7:ktoml-core:0.7.1")
    implementation("com.akuleshov7:ktoml-file:0.7.1")
}

tasks.test {
    useJUnitPlatform()
}

tasks.shadowJar {
    archiveFileName.set("xml_to_pg_etl.jar")
    duplicatesStrategy = DuplicatesStrategy.WARN
    mergeServiceFiles()
    manifest {
        attributes["Main-Class"] = "ru.my.MainKt"
    }
}

kotlin {
    jvmToolchain(21)
}
