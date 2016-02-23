package profiler

/**
  * Created by eugenio on 23/02/16.
  */
class UserCount(counts: Map[String, Int]) {

  def get(query: String) = counts get query

  def contains(query: String) = counts contains query

}

object UserCount {

  def apply(text: String): UserCount = {

    def parseEntry(line: String): List[(String, Int)] = {
      val fields = { line split "\t" }.toList
      val externalId = fields.head
      fields.tail map { field => externalId -> field.toInt }
    }

    val lines = { text split "\n" }.toList
    val counts = lines flatMap parseEntry
    new UserCount(counts.toMap)
  }

}
