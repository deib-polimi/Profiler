package profiler

/**
  * @author Alessandro
  */
class Shuffle(shuffles: Map[String, Record], bytes: Map[String, Long]) {

  def apply(data: Seq[Record]): Seq[Record] = {
    val maps = data filter { _.taskType == MapTask }
    val reduces = data filter { _.taskType == ReduceTask } map {
      task => task - shuffles(task.name)
    }
    val theseShuffles = reduces flatMap {
      task =>
        val newVertex = s"Shuffle ${task.vertex.split(" ").last}"
        bytes get task.name match {
          case Some(value) =>
            Some(shuffles(task.name) copy (taskType = ShuffleTask, vertex = newVertex, bytes = value))
          case None => None
        }
    }
    maps ++ reduces ++ theseShuffles
  }

}

object Shuffle {

  def apply(durationsContent: String, bytesContent: String): Shuffle = {
    val entries = durationsContent split "\n\n" flatMap { _ split "\n" } map
      Record.apply map { x => x.name -> x }
    val bytes = bytesContent split "\n\n" flatMap { _ split "\n" } map {
      line => { line split "\t" }.toList
    } flatMap {
      case name :: data :: Nil => Some(name -> data.toLong)
      case _ => None
    }
    new Shuffle(entries.toMap, bytes.toMap)
  }

}
