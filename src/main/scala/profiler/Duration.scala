package profiler

/**
  * @author Alessandro
  *
  */
case class Duration(durations: Map[String, Long]) {

  private def get(name: String) = durations get name

  def contains(input: String) = durations contains parseIdentifier(input)

  private def parseIdentifier(name: String) = name split "_" slice (1, 3) mkString "_"

  def obtainTotalDuration(names: Seq[String]) = {
    val ids = names.map(parseIdentifier).distinct
    val theseDurations = ids flatMap get
    theseDurations.sum
  }

}

object Duration {

  def apply(text: String): Duration = {
    def parseEntry(text: String): Option[(String, Long)] = {
      val fields = text split "\t"
      val duration = fields(0).toLong
      if (duration < 0) None
      else Some(fields(1).split("_").tail.mkString("_") -> duration)
    }
    val lines = text split "\n"
    Duration(lines.flatMap(parseEntry).toMap)
  }

}
