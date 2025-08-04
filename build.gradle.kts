plugins {
    kotlin("jvm") version "2.1.10"
}

group = "ru.my"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))

    // Корутины (обязательно)
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")

    // HikariCP для connection pooling
    implementation("com.zaxxer:HikariCP:5.0.1")

    // PostgreSQL JDBC driver
    implementation("org.postgresql:postgresql:42.6.0")

    // Jackson для XML
    //implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-xml:2.19.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.19.0")

    // Kotlin reflection (для рефлексии)
    implementation("org.jetbrains.kotlin:kotlin-reflect:1.9.10")

    implementation("org.apache.commons:commons-compress:1.26.1")

    implementation("org.jetbrains.kotlinx:kotlinx-cli:0.3.6")
}

tasks.jar {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    manifest {
        attributes["Main-Class"] = "ru.my.MainKt"
    }

    from({
        configurations.runtimeClasspath.get()
            .filter { it.name.endsWith("jar") }
            .map { zipTree(it) }
    })
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(21)
}
