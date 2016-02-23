package profiler

import java.io.File

import scala.io.Source

/**
  * @author Alessandro
  *
  */
case class Simulation(executions: Seq[Execution]) {

  def avg(taskType: TaskType): Long = executions.map( _ sum taskType ).sum / executions.map( _ numTasks taskType ).sum
  def max(taskType: TaskType): Long = executions.map( _ max taskType ).max
  def min(taskType: TaskType): Long = executions.map( _ min taskType ).min

  def avg(vertex: String): Long = executions.map( _ sum vertex ).sum / executions.map( _ numTasks vertex ).sum
  def max(vertex: String): Long = executions.map( _ max vertex ).max
  def min(vertex: String): Long = executions.map( _ min vertex ).min

  lazy val avg: Long = executions.map( _.duration ).sum / executions.length
  lazy val max: Long = executions.map( _.duration ).max
  lazy val min: Long = executions.map( _.duration ).min

  def all(taskType: TaskType) = executions flatMap { _ tasks taskType }
  def all(vertex: String) = executions flatMap { _ tasks vertex }

  private def extractNumOf[N: Integral](groups: Map[N, Seq[Execution]]) = {
    val counts = groups map { case (key, list) => key -> list.size }
    val maxPair = counts maxBy { case (_, count) => count }
    maxPair._1
  }
  def numOf(taskType: TaskType): Int = extractNumOf(executions groupBy { _ numTasks taskType })
  def numOf(vertex: String): Long = extractNumOf(executions groupBy { _ numTasks vertex })

  val size: Int = executions.length

  lazy val vertices = executions.flatMap(_.vertices).toList.distinct sortBy {
    _.split(" ").last.toInt
  }
  lazy val isNonTrivialDag = executions exists { _.isNonTrivialDag }

  lazy val minShuffleBytes = executions.map( _.minShuffleBytes ).min
  lazy val maxShuffleBytes = executions.map( _.maxShuffleBytes ).max
  lazy val avgShuffleBytes = executions.map( _.sumShuffleBytes ).sum / executions.map( _.numShuffle ).sum

  def minShuffleBytes(vertex: String) = executions.map( _ minShuffleBytes vertex ).min
  def maxShuffleBytes(vertex: String) = executions.map( _ maxShuffleBytes vertex ).max
  def avgShuffleBytes(vertex: String) = executions.map( _ sumShuffleBytes vertex ).sum /
    executions.map( _ numTasks vertex ).sum

}

object Simulation {

  val DEFAULT_ID = "default"

  def fromDir(dir: File): Map[String, Simulation] = {
    val dataDir = new File(dir, "data")
    val durations = Duration(Source.fromFile(new File(dataDir, "appDuration.txt")).mkString)
    val vertices = Vertices(Source.fromFile(new File(dataDir, "vertexLtask.txt")).mkString)
    val idFile = new File(dataDir, "appId.txt")
    val someIds = if (idFile.canRead)
      Some(Identifiers(Source.fromFile(idFile).mkString)) else None

    val shuffleDurations = Source.fromFile(new File(dataDir, "shuffleDurationLO.txt")).mkString
    val shuffleBytes = Source.fromFile(new File(dataDir, "shuffleBytes.txt")).mkString
    val shuffle = Shuffle(shuffleDurations, shuffleBytes)

    val lines = Source.fromFile(new File(dataDir, "taskDurationLO.txt")).mkString
    val blocks = { lines split "\n\n" }.toSeq
    someIds match {
      case Some(identifiers) =>
        blocks groupBy {
          block =>
            val firstLine = { block split "\n" }.head split "\t"
            identifiers get firstLine.head
        } flatMap {
          case (Some(id), theseBlocks) =>
            Some(id -> Simulation(theseBlocks, durations, shuffle, vertices))
          case (None, _) => None
        }
      case None => Map(DEFAULT_ID -> Simulation(blocks, durations, shuffle, vertices))
    }
  }

  def apply(blocks: Seq[String], duration: Duration, shuffle: Shuffle,
            vertices: Vertices): Simulation = {
    val executions = blocks map { Execution(_, duration, shuffle, vertices) } filter
      { duration contains _.taskId }
    executions foreach {
      x =>
        Console.err println
          s"Map tasks: ${ x numTasks MapTask } Reduce tasks: ${ x numTasks ReduceTask }"
    }
    Simulation(executions)
  }
}
