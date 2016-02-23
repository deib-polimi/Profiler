package profiler

/**
  * Created by eugenio on 25/01/16.
  */
case class Identifiers(ids: Map[String, String]) {

  def get(input: String) = ids get parseIdentifier(input)

  def contains(input: String) = ids contains parseIdentifier(input)

  private def parseIdentifier(name: String) = name split "_" slice(1, 3) mkString "_"

}

object Identifiers {

  def apply(text: String): Identifiers = {

    def parseEntry(line: String): List[(String, String)] = {
      val fields = { line split "\t" }.toList
      val externalId = fields.head
      fields.tail map { field => field.split("_").tail.mkString("_") -> externalId }
    }

    val lines = { text split "\n" }.toList
    val ids = lines flatMap parseEntry
    Identifiers(ids.toMap)
  }

}
