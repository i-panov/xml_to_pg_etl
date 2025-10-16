package ru.my

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.io.TempDir
import ru.my.xml.isXmlFile
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.exists
import kotlin.test.Test
import kotlin.test.assertTrue

class ArchiveTest {
    private val currentDir = Paths.get("").toAbsolutePath()
    private val archivesDir = currentDir.resolve("src/test/resources/archives/")

    @TempDir
    lateinit var tempDir: Path

    @Test
    fun extractArchiveTest() {
        val archiveFile = archivesDir.resolve("gar_delta_xml.zip")
        assertTrue { archiveFile.exists() }

        val extractedFiles = runBlocking {
            extractArchive(
                archiveFile = archiveFile,
                extractDir = tempDir,
                checkerFileNameForExtract = { isXmlFile(it) },
            ).toList()
        }

        println(extractedFiles)
        assertTrue { extractedFiles.isNotEmpty() }
    }
}
