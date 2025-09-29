package ru.my

import com.ctc.wstx.exc.WstxParsingException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.writeText

class XmlParserTest {
    @TempDir
    lateinit var tempDir: Path

    private fun writeTempFile(name: String, content: String): Path =
        tempDir.resolve(name).apply { writeText(content.trimIndent()) }

    private fun writeTempFileBytes(name: String, content: String, charset: Charset): Path =
        tempDir.resolve(name).apply { toFile().writeBytes(content.toByteArray(charset)) }

    private val simpleXmlFile: Path by lazy {
        writeTempFile(
            "simple.xml",
            """
        <root>
            <item>Value 1</item>
            <item>Value 2</item>
        </root>
        """
        )
    }

    private val nestedXmlFile: Path by lazy {
        writeTempFile(
            "nested.xml",
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
        """
        )
    }

    private val cdataXmlFile: Path by lazy {
        writeTempFile(
            "cdata.xml",
            """
        <data>
            <message><![CDATA[This is some <b>HTML</b> content.]]></message>
            <description>Regular text</description>
            <empty_cdata><![CDATA[]]></empty_cdata>
        </data>
        """
        )
    }

    private val emptyXmlFile: Path by lazy {
        writeTempFile("empty.xml", "<root/>")
    }

    private val malformedXmlFile: Path by lazy {
        writeTempFile(
            "malformed.xml",
            """
        <root>
            <item>Value 1
            <item>Value 2</item>
        </root>
        """
        )
    }

    private val largeXmlFile: Path by lazy {
        tempDir.resolve("large.xml").apply {
            val largeXmlContent = StringBuilder("<root>")
            for (i in 0 until 10000) {
                largeXmlContent.append("<item id=\"$i\">Value $i</item>")
            }
            largeXmlContent.append("</root>")
            writeText(largeXmlContent.toString())
        }
    }

    private val encodingXmlFileUtf16: Path by lazy {
        writeTempFileBytes(
            "encoding_utf16.xml",
            "\uFEFF<?xml version=\"1.0\" encoding=\"UTF-16\"?><root><item>Привет</item></root>",
            StandardCharsets.UTF_16
        )
    }

    private val encodingXmlFileWin1251: Path by lazy {
        writeTempFileBytes(
            "encoding_win1251.xml",
            "<?xml version=\"1.0\" encoding=\"windows-1251\"?><root><item>Привет</item></root>",
            Charset.forName("windows-1251")
        )
    }

    private val flatXmlFile: Path by lazy {
        writeTempFile(
            "flat.xml",
            """
        <data>
            <record key="1" type="A">Value A1</record>
            <record key="2" type="B">Value B2</record>
            <record key="3" type="A">Value A3</record>
        </data>
        """
        )
    }

    private val missingRequiredXmlFile: Path by lazy {
        writeTempFile(
            "missing_required.xml",
            """
        <products>
            <product id="P1"><name>Item A</name><price>100</price></product>
            <product id="P2"><name></name><price>200</price></product> <!-- Missing name content -->
            <product><name>Item C</name><price>300</price></product> <!-- Missing id attribute -->
            <product id="P4"><name>Item D</name></product> <!-- Missing price content (not required) -->
        </products>
        """
        )
    }

    private val enumFilterXmlFile: Path by lazy {
        writeTempFile(
            "enum_filter.xml",
            """
        <products>
            <product id="A1" category="Electronics"><name lang="en">Laptop</name></product>
            <product id="B2" category="Books"><name lang="fr">Livre</name></product>
            <product id="C3" category="Furniture"><name lang="de">Stuhl</name></product> <!-- Should be filtered -->
        </products>
        """
        )
    }

    // --- Добавленные XML файлы для новых тестов ---
    private val optionalFieldsXmlFile: Path by lazy {
        writeTempFile(
            "optional_fields.xml",
            """
        <items>
            <item id="1">
                <name>Item A</name>
                <description>Description A</description>
            </item>
            <item id="2">
                <name>Item B</name>
                <!-- description is missing -->
            </item>
            <item id="3" type="special">
                <name>Item C</name>
                <description></description> <!-- empty description -->
            </item>
            <item id="4" type="">
                <name>Item D</name>
            </item>
            <item id="5">
                <name>Item E</name>
                <comment>   </comment> <!-- whitespace only content -->
            </item>
        </items>
        """
        )
    }

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
            XmlValueConfig(
                path = listOf("chapters", "chapter", "num"),
                valueType = XmlValueType.ATTRIBUTE,
                outputKey = "chapterNum"
            )
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
            XmlValueConfig(
                path = listOf("id"),
                valueType = XmlValueType.ATTRIBUTE,
                outputKey = "productId",
                required = true
            ),
            // path = listOf("name") означает контент дочернего элемента "name" внутри "product"
            XmlValueConfig(
                path = listOf("name"),
                valueType = XmlValueType.CONTENT,
                outputKey = "productName",
                required = true
            ),
            // path = listOf("price") означает контент дочернего элемента "price" внутри "product"
            XmlValueConfig(
                path = listOf("price"),
                valueType = XmlValueType.CONTENT,
                outputKey = "productPrice"
            ) // Not required
        )

        val records = parseXmlElements(missingRequiredXmlFile, rootPath, valueConfigs).toList()

        assertEquals(2, records.size)
        assertEquals(mapOf("productId" to "P1", "productName" to "Item A", "productPrice" to "100"), records[0])
        assertEquals(
            mapOf("productId" to "P4", "productName" to "Item D"),
            records[1]
        ) // Price is not required, so it's fine
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
            XmlValueConfig(path = listOf("description"), valueType = XmlValueType.CONTENT, outputKey = "description"),
            XmlValueConfig(path = listOf("empty_cdata"), valueType = XmlValueType.CONTENT, outputKey = "emptyCdata")
        )

        val records = parseXmlElements(cdataXmlFile, rootPath, valueConfigs).toList()
        assertEquals(1, records.size)
        assertEquals(
            mapOf(
                "message" to "This is some <b>HTML</b> content.",
                "description" to "Regular text"
                // emptyCdata не будет присутствовать, так как контент пустой и isNotBlank() == false
            ),
            records[0]
        )
        assertFalse(records[0].containsKey("emptyCdata"))
    }

    @Test
    fun `parseXmlElements with specific UTF-16 encoding`() {
        val rootPath = listOf("root", "item") // Абсолютный путь к элементу "item"
        val valueConfigs = setOf(
            // path = listOf() означает контент самого элемента "item"
            XmlValueConfig(path = listOf(), valueType = XmlValueType.CONTENT, outputKey = "itemContent")
        )

        val records =
            parseXmlElements(encodingXmlFileUtf16, rootPath, valueConfigs, encoding = StandardCharsets.UTF_16).toList()
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

        val records = parseXmlElements(
            encodingXmlFileWin1251,
            rootPath,
            valueConfigs,
            encoding = Charset.forName("windows-1251")
        ).toList()
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

    // --- НОВЫЕ ТЕСТЫ ---

    @Test
    fun `parseXmlElements should handle empty XML file with root tag correctly`() {
        val rootPath = listOf("root")
        val valueConfigs = setOf(
            XmlValueConfig(path = listOf(), valueType = XmlValueType.CONTENT, outputKey = "rootContent")
        )

        val records = parseXmlElements(emptyXmlFile, rootPath, valueConfigs).toList()
        assertTrue(records.isEmpty(), "Expected no records for an empty root tag with content extraction")

        val attributeConfigs = setOf(
            XmlValueConfig(path = listOf("attr"), valueType = XmlValueType.ATTRIBUTE, outputKey = "rootAttr")
        )
        val recordsWithAttr = parseXmlElements(emptyXmlFile, rootPath, attributeConfigs).toList()
        assertTrue(recordsWithAttr.isEmpty(), "Expected no records for an empty root tag with attribute extraction")
    }

    @Test
    fun `parseXmlElements should handle missing optional attributes and content`() {
        val rootPath = listOf("items", "item")
        val valueConfigs = setOf(
            XmlValueConfig(path = listOf("id"), valueType = XmlValueType.ATTRIBUTE, outputKey = "itemId", required = true),
            XmlValueConfig(path = listOf("type"), valueType = XmlValueType.ATTRIBUTE, outputKey = "itemType", required = false), // Optional attribute
            XmlValueConfig(path = listOf("name"), valueType = XmlValueType.CONTENT, outputKey = "itemName", required = true),
            XmlValueConfig(path = listOf("description"), valueType = XmlValueType.CONTENT, outputKey = "itemDescription", required = false), // Optional content
            XmlValueConfig(path = listOf("comment"), valueType = XmlValueType.CONTENT, outputKey = "itemComment", required = false) // Optional content, whitespace only
        )

        val records = parseXmlElements(optionalFieldsXmlFile, rootPath, valueConfigs).toList()

        assertEquals(5, records.size)

        // Item 1: All present
        assertEquals(mapOf("itemId" to "1", "itemName" to "Item A", "itemDescription" to "Description A"), records[0])
        assertFalse(records[0].containsKey("itemType"))
        assertFalse(records[0].containsKey("itemComment"))

        // Item 2: Missing optional description
        assertEquals(mapOf("itemId" to "2", "itemName" to "Item B"), records[1])
        assertFalse(records[1].containsKey("itemType"))
        assertFalse(records[1].containsKey("itemDescription"))
        assertFalse(records[1].containsKey("itemComment"))

        // Item 3: Empty optional description, optional attribute "type" present
        assertEquals(mapOf("itemId" to "3", "itemName" to "Item C", "itemType" to "special"), records[2])
        assertFalse(records[2].containsKey("itemDescription")) // Empty content is not added
        assertFalse(records[2].containsKey("itemComment"))

        // Item 4: Empty optional attribute "type"
        assertEquals(mapOf("itemId" to "4", "itemName" to "Item D", "itemType" to ""), records[3])
        assertFalse(records[3].containsKey("itemDescription"))
        assertFalse(records[3].containsKey("itemComment"))

        // Item 5: Whitespace only optional comment
        assertEquals(mapOf("itemId" to "5", "itemName" to "Item E"), records[4])
        assertFalse(records[4].containsKey("itemType"))
        assertFalse(records[4].containsKey("itemDescription"))
        assertFalse(records[4].containsKey("itemComment")) // Whitespace only content is not added
    }

    @Test
    fun `parseXmlElements should throw IllegalArgumentException for duplicate output keys`() {
        val rootPath = listOf("root", "item")
        val valueConfigs = setOf(
            XmlValueConfig(path = listOf(), valueType = XmlValueType.CONTENT, outputKey = "duplicateKey"),
            XmlValueConfig(path = listOf("id"), valueType = XmlValueType.ATTRIBUTE, outputKey = "duplicateKey")
        )

        val exception = assertThrows(IllegalArgumentException::class.java) {
            parseXmlElements(simpleXmlFile, rootPath, valueConfigs).toList()
        }
        assertTrue(exception.message?.contains("Duplicate output keys found in valueConfigs") ?: false)
    }

    @Test
    fun `parseXmlElements addr_obj_types`() {
        val path = xmlDir.resolve("addr_obj_types.xml")

        val items = parseXmlElements(
            file = path,
            rootPath = listOf("ADDRESSOBJECTTYPES", "ADDRESSOBJECTTYPE"),
            valueConfigs = setOf(
                XmlValueConfig(path = listOf("ID"), valueType = XmlValueType.ATTRIBUTE, outputKey = "id"),
                XmlValueConfig(path = listOf("NAME"), valueType = XmlValueType.ATTRIBUTE, outputKey = "NAME"),
                XmlValueConfig(path = listOf("SHORTNAME"), valueType = XmlValueType.ATTRIBUTE, outputKey = "shortname"),
                XmlValueConfig(path = listOf("DESC"), valueType = XmlValueType.ATTRIBUTE, outputKey = "DESC"),
                XmlValueConfig(path = listOf("ISACTIVE"), valueType = XmlValueType.ATTRIBUTE, outputKey = "isactive"),
                XmlValueConfig(path = listOf("ENDDATE"), valueType = XmlValueType.ATTRIBUTE, outputKey = "enddate"),
                XmlValueConfig(path = listOf("LEVEL"), valueType = XmlValueType.ATTRIBUTE, outputKey = "LEVEL"),
            ),
        ).toList()

        assertEquals(427, items.size)
        println(items.first())
    }

    private val currentDir = Paths.get("").toAbsolutePath()
    private val xmlDir = currentDir.resolve("src/test/resources/xml/")
}
