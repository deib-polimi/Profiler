package profiler

/**
  * @author Alessandro
  *
  */
abstract case class Execution(name: String, tasks: Seq[Record]) {

  lazy val numMap: Int = numTasks(MapTask)
  lazy val numReduce: Int = numTasks(ReduceTask)
  lazy val numShuffle: Int = numTasks(ShuffleTask)

  val duration: Long

  lazy val locations: Set[String] = tasks.map(_.location).toSet

  def tasks(taskType: TaskType): Seq[Record] = tasks filter (_.taskType == taskType)
  def numTasks(taskType: TaskType): Int = tasks(taskType).length
  def sum(taskType: TaskType): Long = tasks(taskType).map(_.durationMSec).sum
  def max(taskType: TaskType): Long = tasks(taskType).map(_.durationMSec).max
  def min(taskType: TaskType): Long = tasks(taskType).map(_.durationMSec).min
  def avg(taskType: TaskType): Long = sum(taskType) / numTasks(taskType)

  def tasks(vertex: String): Seq[Record]
  def numTasks(vertex: String): Long
  def sum(vertex: String): Long
  def max(vertex: String): Long
  def min(vertex: String): Long
  def avg(vertex: String): Long

  val vertices: List[String]
  val isNonTrivialDag: Boolean

  lazy val shuffleBytes: Seq[Long] = tasks(ShuffleTask) map { _.bytes }
  lazy val sumShuffleBytes: Long = shuffleBytes.sum
  lazy val maxShuffleBytes: Long = shuffleBytes.max
  lazy val minShuffleBytes: Long = shuffleBytes.min
  lazy val avgShuffleBytes: Long = sumShuffleBytes / numShuffle

  def shuffleBytes(vertex: String): Seq[Long]
  def sumShuffleBytes(vertex: String): Long
  def maxShuffleBytes(vertex: String): Long
  def minShuffleBytes(vertex: String): Long
  def avgShuffleBytes(vertex: String): Long

}

object Execution {

  def apply(text: String, duration: Duration, shuffle: Shuffle, vertices: Vertices): Execution = {
    val lines = text split "\n"
    new ExternalExecution(lines.head, duration, shuffle(vertices(lines map Record.apply)))
  }

}
