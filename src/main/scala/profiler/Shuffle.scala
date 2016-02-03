package profiler

/**
  * @author Alessandro
  */
case class Shuffle(shuffles: Map[String, Record]) {

  def apply(data: Seq[Record]): Seq[Record] = {
    val maps = data filter { _.taskType == MapTask }
    val reduces = data filter { _.taskType == ReduceTask } map {
      task => task - shuffles(task.name)
    }
    val theseShuffles = reduces map {
      task =>
        val newVertex = s"Shuffle ${task.vertex.split(" ").last}"
        shuffles(task.name) changeRecord (ShuffleTask, newVertex)
    }
    maps ++ reduces ++ theseShuffles
  }

}

object Shuffle{

  def apply(input: String): Shuffle = {
    val entries = input split "\n\n" flatMap { _ split "\n" } map
      Record.apply map { x => x.name -> x }
    Shuffle(entries.toMap)
  }

}
