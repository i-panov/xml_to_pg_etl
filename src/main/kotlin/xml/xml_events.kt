package ru.my.xml

import java.io.BufferedReader
import java.nio.charset.Charset
import java.nio.file.Path
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants.*
import javax.xml.stream.XMLStreamReader
import kotlin.io.path.inputStream

sealed class XmlEvent(
    name: String, // Этот "хак" нужен, чтобы обойти порядок инициализации В Котлин
    open val level: Int,
    open val localIndex: Long,
    open val globalIndex: Long,
) {
    abstract val name: String

    init {
        require(name.isNotBlank()) { "Event name cannot be blank" }
        require(level >= 0) { "Event level cannot be negative" }
        require(localIndex >= 0) { "Event localIndex cannot be negative" }
        require(globalIndex >= 0) { "Event globalIndex cannot be negative" }
    }
}

data class StartElementEvent(
    override val name: String,
    override val level: Int,
    override val localIndex: Long,
    override val globalIndex: Long,
    val attributes: Map<String, String> = emptyMap(),
) : XmlEvent(name, level, localIndex, globalIndex)

data class CharactersEvent(
    override val name: String,
    override val level: Int,
    override val localIndex: Long,
    override val globalIndex: Long,
    val content: String,
    val isCData: Boolean = false,
) : XmlEvent(name, level, localIndex, globalIndex) {
    init {
        require(content.isNotBlank()) { "Characters content cannot be blank" }
    }
}

data class EndElementEvent(
    override val name: String,
    override val level: Int,
    override val localIndex: Long,
    override val globalIndex: Long,
) : XmlEvent(name, level, localIndex, globalIndex)

private data class ElementState(
    val name: String,
    val level: Int,
    val localIndex: Long,
    val globalIndex: Long,
    var childCounter: Int = 0
) {
    init {
        require(name.isNotBlank()) { "ElementState name cannot be blank" }
        require(level >= 0) { "ElementState level cannot be negative" }
        require(localIndex >= 0) { "ElementState localIndex cannot be negative" }
        require(globalIndex >= 0) { "ElementState globalIndex cannot be negative" }
        require(childCounter >= 0) { "ElementState childCounter cannot be negative" }
    }
}

fun XMLStreamReader.iterateXml(): Sequence<XmlEvent> {
    return sequence {
        val elementStack = ArrayDeque<ElementState>()
        var globalIndexCounter = 0L

        fun cleanName(rawName: String): String {
            val lastColonIndex = rawName.lastIndexOf(':')
            if (lastColonIndex == -1) return rawName
            return rawName.substring(lastColonIndex + 1)
        }

        while (hasNext()) {
            val eventType = next()
            val currentGlobalIndex = globalIndexCounter++

            val eventToYield: XmlEvent? = when (eventType) {
                START_ELEMENT -> {
                    val currentName = cleanName(localName)
                    val currentLevel = elementStack.size

                    val localIndex: Long
                    if (elementStack.isNotEmpty()) {
                        val parentState = elementStack.last()
                        localIndex = parentState.childCounter.toLong()
                        parentState.childCounter++
                    } else {
                        localIndex = 0L
                    }

                    val attributes = (0 until attributeCount).associate { i ->
                        val attrRawName = getAttributeLocalName(i)
                        val attrKey = cleanName(attrRawName) // Очистка ключа
                        attrKey to getAttributeValue(i).trim()
                    }

                    elementStack.addLast(ElementState(currentName, currentLevel, localIndex, currentGlobalIndex))

                    StartElementEvent(
                        name = currentName,
                        attributes = attributes,
                        level = currentLevel,
                        localIndex = localIndex,
                        globalIndex = currentGlobalIndex,
                    )
                }
                CHARACTERS, CDATA -> {
                    val currentText = text.trim()
                    if (currentText.isNotBlank() && elementStack.isNotEmpty()) {
                        val currentElementState = elementStack.last()
                        CharactersEvent(
                            name = currentElementState.name,
                            level = currentElementState.level,
                            localIndex = currentElementState.localIndex,
                            globalIndex = currentGlobalIndex,
                            content = currentText,
                            isCData = eventType == CDATA,
                        )
                    } else {
                        null
                    }
                }
                END_ELEMENT -> {
                    val cleanedLocalName = cleanName(localName)

                    if (elementStack.isNotEmpty()) {
                        val closedElementState = elementStack.removeLast()

                        if (closedElementState.name != cleanedLocalName) {
                            throw XmlParsingException(
                                "Mismatched closing tag: expected '${closedElementState.name}', got '$cleanedLocalName' (raw: '$localName')"
                            )
                        }

                        EndElementEvent(
                            name = closedElementState.name,
                            level = closedElementState.level,
                            localIndex = closedElementState.localIndex,
                            globalIndex = currentGlobalIndex,
                        )
                    } else {
                        throw XmlParsingException("Unexpected closing tag: '$cleanedLocalName' (raw: '$localName') without opening tag")
                    }
                }
                else -> null
            }

            if (eventToYield != null) {
                yield(eventToYield)
            }
        }
    }
}

fun Path.iterateXml(charset: Charset = Charsets.UTF_8, skipBytes: Int = 0): Sequence<XmlEvent> {
    return sequence {
        inputStream().use { inputStream ->
            if (skipBytes > 0) {
                inputStream.skip(skipBytes.toLong())
            }

            val bufferedStream = inputStream.buffered()
            val reader = bufferedStream.reader(charset)
            val bufferedReader = BufferedReader(reader, 8192)
            val xmlReader = xmlInputFactory.createXMLStreamReader(bufferedReader)

            try {
                yieldAll(xmlReader.iterateXml())
            } finally {
                xmlReader.close()
            }
        }
    }
}

class XmlParsingException(message: String, cause: Exception?) : Exception(message, cause) {
    constructor(message: String) : this(message, null)
}

private val xmlInputFactory = XMLInputFactory.newInstance().apply {
    setProperty(XMLInputFactory.IS_COALESCING, true)
    setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
    setProperty(XMLInputFactory.IS_VALIDATING, false)
    setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false)
    setProperty(XMLInputFactory.SUPPORT_DTD, false)
}
