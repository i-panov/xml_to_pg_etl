package ru.my.xml

data class XmlNode(
    val name: String,
    val attributes: Map<String, String> = emptyMap(),
    val children: List<XmlNode> = emptyList(),
    val contentLines: List<XmlContentLine> = emptyList(),
) {
    val content by lazy { contentLines.joinToString("\n") { it.line.trim() }.trim() }
}

data class XmlContentLine(
    val line: String,
    val isCData: Boolean = false,
)

/**
 * Вспомогательный класс для построения XmlNode
 */
private class NodeBuilder(
    val name: String,
    val attributes: Map<String, String>,
) {
    val children = mutableListOf<XmlNode>()
    val contentLines = mutableListOf<XmlContentLine>()

    fun build(): XmlNode {
        return XmlNode(
            name = name,
            attributes = attributes,
            children = children.toList(),
            contentLines = contentLines.toList()
        )
    }
}

/**
 * Преобразует последовательность XML событий в последовательность узлов.
 *
 * @param rootPath путь к элементам, которые нужно извлечь.
 *                 Например, ["root", "items", "item"] извлечёт все <item> элементы
 *                 внутри <root><items>
 *                 Пустой список означает извлечение элементов верхнего уровня.
 * @return последовательность XmlNode для элементов по указанному пути
 */
fun Iterable<XmlEvent>.toNodes(rootPath: List<String>): Sequence<XmlNode> = sequence {
    val currentPath = mutableListOf<String>() // Текущий путь в XML дереве
    val nodeStack = ArrayDeque<NodeBuilder>() // Стек для построения узлов

    for (event in this@toNodes) {
        when (event) {
            is StartElementEvent -> {
                // Добавляем элемент в текущий путь
                currentPath.add(event.name)

                val shouldStartNode = if (rootPath.isEmpty()) {
                    // Если rootPath пустой, извлекаем элементы верхнего уровня
                    event.level == 0
                } else {
                    // Проверяем, совпадает ли текущий путь с rootPath
                    // Это автоматически гарантирует правильный уровень
                    currentPath == rootPath
                }

                if (shouldStartNode) {
                    // Начинаем новый корневой узел
                    nodeStack.addLast(NodeBuilder(event.name, event.attributes))
                } else if (nodeStack.isNotEmpty()) {
                    // Мы внутри узла, который строим - добавляем дочерний элемент
                    nodeStack.addLast(NodeBuilder(event.name, event.attributes))
                }
            }

            is CharactersEvent -> {
                // Если мы строим узел, добавляем контент
                if (nodeStack.isNotEmpty()) {
                    val currentBuilder = nodeStack.last()
                    currentBuilder.contentLines.add(
                        XmlContentLine(
                            line = event.content,
                            isCData = event.isCData
                        )
                    )
                }
            }

            is EndElementEvent -> {
                // Завершаем текущий элемент
                if (nodeStack.isNotEmpty()) {
                    val completedBuilder = nodeStack.removeLast()
                    val completedNode = completedBuilder.build()

                    if (nodeStack.isEmpty()) {
                        // Это корневой узел по нашему пути - возвращаем его
                        yield(completedNode)
                    } else {
                        // Это дочерний узел - добавляем к родителю
                        nodeStack.last().children.add(completedNode)
                    }
                }

                // Удаляем элемент из текущего пути
                if (currentPath.isNotEmpty() && currentPath.last() == event.name) {
                    currentPath.removeLast()
                }
            }
        }
    }
}
