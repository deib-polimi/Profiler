package profiler

/**
 * @author eugenio
 */
case class Vertices(assignments : Map[String, String]) {

  def get(name : String) = assignments get parseIdentifier(name)

  def contains(name : String) = assignments contains parseIdentifier(name)

  private def parseIdentifier(name : String) = {name split "_"}.tail mkString "_"

  private def inferType(name : String) = get(name) match {
    case Some(vertex) if vertex startsWith "Map" => MapTask
    case Some(vertex) if vertex startsWith "Reduce" => ReduceTask
    case Some(vertex) => throw new RuntimeException(s"unrecognized vertex type: $vertex")
    case None =>
      val missing = parseIdentifier(name)
      throw new RuntimeException(s"missing vertex assignment for task: $missing")
  }

  def apply(records : Array[Record]) : Array[Record] = for (record <- records) yield {
    val taskType = inferType(record.name)
    record changeType taskType
  }

}

object Vertices {

  def apply(text : String) : Vertices = {
    def parseLine(line : String) : Map[String, String] = {
      val tokens = line split "\t"
      val vertex = tokens.head
      val attempts = tokens.tail
      val couples = for (attempt <- attempts) yield {
        val identifier = {attempt split "_"}.tail mkString "_"
        identifier -> vertex
      }
      couples.toMap
    }
    val lines = text split "\n"
    Vertices({lines flatMap parseLine}.toMap)
  }
}
