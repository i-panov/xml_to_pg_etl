package ru.my

import com.ctc.wstx.exc.WstxParsingException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import kotlin.io.path.writeText

class XmlParserTest {
    @TempDir
    lateinit var tempDir: Path

    private lateinit var simpleXmlFile: Path
    private lateinit var nestedXmlFile: Path
    private lateinit var cdataXmlFile: Path
    private lateinit var attributesXmlFile: Path
    private lateinit var emptyXmlFile: Path
    private lateinit var malformedXmlFile: Path
    private lateinit var largeXmlFile: Path
    private lateinit var encodingXmlFileUtf16: Path
    private lateinit var encodingXmlFileWin1251: Path
    private lateinit var flatXmlFile: Path
    private lateinit var missingRequiredXmlFile: Path
    private lateinit var enumFilterXmlFile: Path

    @BeforeEach
    fun setUp() {
        simpleXmlFile = tempDir.resolve("simple.xml")
        simpleXmlFile.writeText(
            """
            <root>
                <item>Value 1</item>
                <item>Value 2</item>
            </root>
            """.trimIndent()
        )

        nestedXmlFile = tempDir.resolve("nested.xml")
        nestedXmlFile.writeText(
            """
            <library>
                <book id="1">
                    <title>The Great Adventure</title>
                    <author>John Doe</author>
                    <chapters>
                        <chapter num="1">Beginning</chapter>
                        <chapter num="2">Middle</chapter>
                    </chapters>
                </book>
                <book id="2">
                    <title>Kotlin for Dummies</title>
                    <author>Jane Smith</author>
                </book>
            </library>
            """.trimIndent()
        )

        cdataXmlFile = tempDir.resolve("cdata.xml")
        cdataXmlFile.writeText(
            """
            <data>
                <message><![CDATA[This is some <b>HTML</b> content.]]></message>
                <description>Regular text</description>
            </data>
            """.trimIndent()
        )

        attributesXmlFile = tempDir.resolve("attributes.xml")
        attributesXmlFile.writeText(
            """
            <products>
                <product id="A1" category="Electronics">
                    <name lang="en">Laptop</name>
                    <price currency="USD">1200.00</price>
                </product>
                <product id="B2" category="Books">
                    <name lang="fr">Livre</name>
                    <price currency="EUR">25.50</price>
                </product>
            </products>
            """.trimIndent()
        )

        emptyXmlFile = tempDir.resolve("empty.xml")
        emptyXmlFile.writeText("<root/>")

        malformedXmlFile = tempDir.resolve("malformed.xml")
        malformedXmlFile.writeText(
            """
            <root>
                <item>Value 1
                <item>Value 2</item>
            </root>
            """.trimIndent()
        )

        largeXmlFile = tempDir.resolve("large.xml")
        val largeXmlContent = StringBuilder("<root>")
        for (i in 0 until 10000) {
            largeXmlContent.append("<item id=\"$i\">Value $i</item>")
        }
        largeXmlContent.append("</root>")
        largeXmlFile.writeText(largeXmlContent.toString())

        encodingXmlFileUtf16 = tempDir.resolve("encoding_utf16.xml")
        encodingXmlFileUtf16.toFile().writeBytes(
            "\uFEFF<?xml version=\"1.0\" encoding=\"UTF-16\"?><root><item>Привет</item></root>"
                .toByteArray(StandardCharsets.UTF_16)
        )

        encodingXmlFileWin1251 = tempDir.resolve("encoding_win1251.xml")
        encodingXmlFileWin1251.toFile().writeBytes(
            "<?xml version=\"1.0\" encoding=\"windows-1251\"?><root><item>Привет</item></root>"
                .toByteArray(Charset.forName("windows-1251"))
        )

        flatXmlFile = tempDir.resolve("flat.xml")
        flatXmlFile.writeText(
            """
            <data>
                <record key="1" type="A">Value A1</record>
                <record key="2" type="B">Value B2</record>
                <record key="3" type="A">Value A3</record>
            </data>
            """.trimIndent()
        )

        missingRequiredXmlFile = tempDir.resolve("missing_required.xml")
        missingRequiredXmlFile.writeText(
            """
            <products>
                <product id="P1"><name>Item A</name><price>100</price></product>
                <product id="P2"><name></name><price>200</price></product> <!-- Missing name content -->
                <product><name>Item C</name><price>300</price></product> <!-- Missing id attribute -->
                <product id="P4"><name>Item D</name></product> <!-- Missing price content (not required) -->
            </products>
            """.trimIndent()
        )

        enumFilterXmlFile = tempDir.resolve("enum_filter.xml")
        enumFilterXmlFile.writeText(
            """
            <products>
                <product id="A1" category="Electronics"><name lang="en">Laptop</name></product>
                <product id="B2" category="Books"><name lang="fr">Livre</name></product>
                <product id="C3" category="Furniture"><name lang="de">Stuhl</name></product> <!-- Should be filtered -->
            </products>
            """.trimIndent()
        )
    }

    // region parseXmlElements tests

    @Test
    fun `parseXmlElements should extract content from simple flat XML`() {
        val rootPath = listOf("root", "item") // Абсолютный путь к элементу, который является "записью"
        val valueConfigs = setOf(
            // path = listOf() означает, что мы берем контент самого элемента "item"
            XmlValueConfig(path = listOf(), valueType = XmlValueType.CONTENT, outputKey = "itemContent")
        )

        val records = parseXmlElements(simpleXmlFile, rootPath, valueConfigs).toList()

        assertEquals(2, records.size)
        assertEquals(mapOf("itemContent" to "Value 1"), records[0])
        assertEquals(mapOf("itemContent" to "Value 2"), records[1])
    }

    @Test
    fun `parseXmlElements should extract attributes and content from flat XML`() {
        val rootPath = listOf("data", "record") // Абсолютный путь к элементу "record"
        val valueConfigs = setOf(
            // path = listOf("key") означает атрибут "key" у элемента "record"
            XmlValueConfig(path = listOf("key"), valueType = XmlValueType.ATTRIBUTE, outputKey = "recordKey"),
            // path = listOf("type") означает атрибут "type" у элемента "record"
            XmlValueConfig(path = listOf("type"), valueType = XmlValueType.ATTRIBUTE, outputKey = "recordType"),
            // path = listOf() означает контент самого элемента "record"
            XmlValueConfig(path = listOf(), valueType = XmlValueType.CONTENT, outputKey = "recordValue")
        )

        val records = parseXmlElements(flatXmlFile, rootPath, valueConfigs).toList()

        assertEquals(3, records.size)
        assertEquals(mapOf("recordKey" to "1", "recordType" to "A", "recordValue" to "Value A1"), records[0])
        assertEquals(mapOf("recordKey" to "2", "recordType" to "B", "recordValue" to "Value B2"), records[1])
        assertEquals(mapOf("recordKey" to "3", "recordType" to "A", "recordValue" to "Value A3"), records[2])
    }

    @Test
    fun `parseXmlElements should extract nested content and attributes`() {
        val rootPath = listOf("library", "book") // Абсолютный путь к элементу "book"
        val valueConfigs = setOf(
            // path = listOf("id") означает атрибут "id" у элемента "book"
            XmlValueConfig(path = listOf("id"), valueType = XmlValueType.ATTRIBUTE, outputKey = "bookId"),
            // path = listOf("title") означает контент дочернего элемента "title" внутри "book"
            XmlValueConfig(path = listOf("title"), valueType = XmlValueType.CONTENT, outputKey = "bookTitle"),
            // path = listOf("author") означает контент дочернего элемента "author" внутри "book"
            XmlValueConfig(path = listOf("author"), valueType = XmlValueType.CONTENT, outputKey = "bookAuthor"),
            // path = listOf("chapters", "chapter", "num") означает атрибут "num" у элемента "chapter",
            // который находится внутри "chapters", который находится внутри "book"
            XmlValueConfig(path = listOf("chapters", "chapter", "num"), valueType = XmlValueType.ATTRIBUTE, outputKey = "chapterNum")
        )

        val records = parseXmlElements(nestedXmlFile, rootPath, valueConfigs).toList()

        assertEquals(2, records.size)
        assertEquals(
            mapOf(
                "bookId" to "1",
                "bookTitle" to "The Great Adventure",
                "bookAuthor" to "John Doe",
                "chapterNum" to "2" // Last encountered chapter num
            ),
            records[0]
        )
        assertEquals(
            mapOf(
                "bookId" to "2",
                "bookTitle" to "Kotlin for Dummies",
                "bookAuthor" to "Jane Smith"
            ),
            records[1]
        )
    }

    @Test
    fun `parseXmlElements should filter records based on required fields`() {
        val rootPath = listOf("products", "product") // Абсолютный путь к элементу "product"
        val valueConfigs = setOf(
            // path = listOf("id") означает атрибут "id" у элемента "product"
            XmlValueConfig(path = listOf("id"), valueType = XmlValueType.ATTRIBUTE, outputKey = "productId", required = true),
            // path = listOf("name") означает контент дочернего элемента "name" внутри "product"
            XmlValueConfig(path = listOf("name"), valueType = XmlValueType.CONTENT, outputKey = "productName", required = true),
            // path = listOf("price") означает контент дочернего элемента "price" внутри "product"
            XmlValueConfig(path = listOf("price"), valueType = XmlValueType.CONTENT, outputKey = "productPrice") // Not required
        )

        val records = parseXmlElements(missingRequiredXmlFile, rootPath, valueConfigs).toList()

        assertEquals(2, records.size)
        assertEquals(mapOf("productId" to "P1", "productName" to "Item A", "productPrice" to "100"), records[0])
        assertEquals(mapOf("productId" to "P4", "productName" to "Item D"), records[1]) // Price is not required, so it's fine
    }

    @Test
    fun `parseXmlElements should filter records based on enum values`() {
        val rootPath = listOf("products", "product") // Абсолютный путь к элементу "product"
        val valueConfigs = setOf(
            // path = listOf("category") означает атрибут "category" у элемента "product"
            XmlValueConfig(path = listOf("category"), valueType = XmlValueType.ATTRIBUTE, outputKey = "category"),
            // path = listOf("name") означает контент дочернего элемента "name" внутри "product"
            XmlValueConfig(path = listOf("name"), valueType = XmlValueType.CONTENT, outputKey = "productName")
        )
        val enumValues = mapOf("category" to setOf("Electronics", "Books"))

        val records = parseXmlElements(enumFilterXmlFile, rootPath, valueConfigs, enumValues).toList()
        assertEquals(2, records.size)
        assertEquals(mapOf("category" to "Electronics", "productName" to "Laptop"), records[0])
        assertEquals(mapOf("category" to "Books", "productName" to "Livre"), records[1])
    }

    @Test
    fun `parseXmlElements should handle empty rootPath`() {
        val rootPath = listOf<String>()
        val valueConfigs = setOf(
            // path = listOf() означает, что мы берем контент самого элемента "item"
            XmlValueConfig(path = listOf(), valueType = XmlValueType.CONTENT, outputKey = "itemContent")
        )
        val exception = assertThrows(IllegalArgumentException::class.java) {
            parseXmlElements(simpleXmlFile, rootPath, valueConfigs).toList()
        }
        assertEquals("Root path must not be empty", exception.message)
    }

    @Test
    fun `parseXmlElements should handle empty valueConfigs`() {
        val rootPath = listOf("root", "item")
        val valueConfigs = emptySet<XmlValueConfig>()
        val exception = assertThrows(IllegalArgumentException::class.java) {
            parseXmlElements(simpleXmlFile, rootPath, valueConfigs).toList()
        }
        assertEquals("Value configs must not be empty", exception.message)
    }

    @Test
    fun `parseXmlElements should handle file not found`() {
        val nonExistentFile = tempDir.resolve("non_existent.xml")
        val rootPath = listOf("root", "item")
        val valueConfigs = setOf(
            // path = listOf() означает, что мы берем контент самого элемента "item"
            XmlValueConfig(path = listOf(), valueType = XmlValueType.CONTENT, outputKey = "itemContent")
        )
        val exception = assertThrows(IllegalArgumentException::class.java) {
            parseXmlElements(nonExistentFile, rootPath, valueConfigs).toList()
        }
        assertTrue(exception.message?.startsWith("File not found:") ?: false)
    }

    @Test
    fun `parseXmlElements should handle duplicate paths in valueConfigs`() {
        val rootPath = listOf("root", "item")
        val valueConfigs = setOf(
            // path = listOf() означает, что мы берем контент самого элемента "item"
            XmlValueConfig(path = listOf(), valueType = XmlValueType.CONTENT, outputKey = "itemContent1"),
            XmlValueConfig(path = listOf(), valueType = XmlValueType.CONTENT, outputKey = "itemContent2")
        )
        val exception = assertThrows(IllegalArgumentException::class.java) {
            parseXmlElements(simpleXmlFile, rootPath, valueConfigs).toList()
        }
        assertTrue(exception.message?.contains("Duplicate paths found in valueConfigs") ?: false)
    }

    @Test
    fun `parseXmlElements should correctly handle CDATA content`() {
        val rootPath = listOf("data") // Абсолютный путь к элементу "data"
        val valueConfigs = setOf(
            // path = listOf("message") означает контент дочернего элемента "message" внутри "data"
            XmlValueConfig(path = listOf("message"), valueType = XmlValueType.CONTENT, outputKey = "message"),
            // path = listOf("description") означает контент дочернего элемента "description" внутри "data"
            XmlValueConfig(path = listOf("description"), valueType = XmlValueType.CONTENT, outputKey = "description")
        )

        val records = parseXmlElements(cdataXmlFile, rootPath, valueConfigs).toList()
        assertEquals(1, records.size)
        assertEquals(
            mapOf(
                "message" to "This is some <b>HTML</b> content.",
                "description" to "Regular text"
            ),
            records[0]
        )
    }

    @Test
    fun `parseXmlElements with specific UTF-16 encoding`() {
        val rootPath = listOf("root", "item") // Абсолютный путь к элементу "item"
        val valueConfigs = setOf(
            // path = listOf() означает контент самого элемента "item"
            XmlValueConfig(path = listOf(), valueType = XmlValueType.CONTENT, outputKey = "itemContent")
        )

        val records = parseXmlElements(encodingXmlFileUtf16, rootPath, valueConfigs, encoding = StandardCharsets.UTF_16).toList()
        assertEquals(1, records.size)
        assertEquals(mapOf("itemContent" to "Привет"), records[0])
    }

    @Test
    fun `parseXmlElements with specific windows-1251 encoding`() {
        val rootPath = listOf("root", "item") // Абсолютный путь к элементу "item"
        val valueConfigs = setOf(
            // path = listOf() означает контент самого элемента "item"
            XmlValueConfig(path = listOf(), valueType = XmlValueType.CONTENT, outputKey = "itemContent")
        )

        val records = parseXmlElements(encodingXmlFileWin1251, rootPath, valueConfigs, encoding = Charset.forName("windows-1251")).toList()
        assertEquals(1, records.size)
        assertEquals(mapOf("itemContent" to "Привет"), records[0])
    }

    @Test
    fun `parseXmlElements should handle large XML file efficiently`() {
        val rootPath = listOf("root", "item") // Абсолютный путь к элементу "item"
        val valueConfigs = setOf(
            // path = listOf("id") означает атрибут "id" у элемента "item"
            XmlValueConfig(path = listOf("id"), valueType = XmlValueType.ATTRIBUTE, outputKey = "itemId"),
            // path = listOf() означает контент самого элемента "item"
            XmlValueConfig(path = listOf(), valueType = XmlValueType.CONTENT, outputKey = "itemValue")
        )

        val records = parseXmlElements(largeXmlFile, rootPath, valueConfigs).toList()
        assertEquals(10000, records.size)
        assertEquals(mapOf("itemId" to "0", "itemValue" to "Value 0"), records.first())
        assertEquals(mapOf("itemId" to "9999", "itemValue" to "Value 9999"), records.last())
    }

    @Test
    fun `parseXmlElements should throw XmlParsingException for malformed XML`() {
        val rootPath = listOf("root", "item") // Абсолютный путь к элементу "item"
        val valueConfigs = setOf(
            // path = listOf() означает контент самого элемента "item"
            XmlValueConfig(path = listOf(), valueType = XmlValueType.CONTENT, outputKey = "itemContent")
        )

        val exception = assertThrows(WstxParsingException::class.java) {
            parseXmlElements(malformedXmlFile, rootPath, valueConfigs).toList()
        }
        assertTrue(exception.message?.contains("Mismatched closing tag") ?: false || exception.message?.contains("Unexpected close tag") ?: false)
    }
}
