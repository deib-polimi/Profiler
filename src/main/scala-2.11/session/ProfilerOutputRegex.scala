package session

object ProfilerOutputRegex {

  val avgMap = """Avg MAP: (\d+)""".r
  val maxMap = """Max MAP: (\d+)""".r
  val avgReduce = """Avg REDUCE: (\d+)""".r
  val maxReduce = """Max REDUCE: (\d+)""".r
  val avgShuffle = """Avg SHUFFLE: (\d+)""".r
  val maxShuffle = """Max SHUFFLE: (\d+)""".r
  val numMap = """MAP tasks: (\d+)""".r
  val numReduce = """REDUCE tasks: (\d+)""".r

}
