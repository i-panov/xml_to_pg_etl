package ru.my

import java.io.File
import java.util.logging.Logger
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants.*

private val logger = Logger.getLogger("XmlParser")

fun parseXmlElements(file: File, tag: String): Sequence<Map<String, String>> = sequence {
    if (!file.exists()) {
        logger.warning("File ${file.name} not found")
        return@sequence
    }

    val xmlInputFactory = XMLInputFactory.newInstance().apply {
        setProperty(XMLInputFactory.IS_COALESCING, true)
        setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
        setProperty(XMLInputFactory.IS_VALIDATING, false)
        setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false)
        setProperty(XMLInputFactory.SUPPORT_DTD, false)
    }

    var processedCount = 0

    try {
        file.inputStream().use { inputStream ->
            val reader = xmlInputFactory.createXMLStreamReader(inputStream)

            while (reader.hasNext()) {
                if (reader.next() == START_ELEMENT && reader.localName == tag) {
                    val attributes = (0 until reader.attributeCount).associate { i ->
                        reader.getAttributeLocalName(i) to (reader.getAttributeValue(i)?.trim() ?: "")
                    }

                    if (attributes.isNotEmpty()) {
                        yield(attributes)
                        processedCount++

                        if (processedCount % 10000 == 0) {
                            logger.info("Processed $processedCount records from ${file.name}")
                        }
                    }
                }
            }
            reader.close()
        }

        logger.info("Completed parsing ${file.name}: $processedCount records")

    } catch (e: Exception) {
        logger.severe("Error parsing file ${file.name}: ${e.message}")
        throw e
    }
}
