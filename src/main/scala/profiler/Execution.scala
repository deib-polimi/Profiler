/**
  *
  */
package profiler

/**
  * @author Alessandro
  *
  */
abstract case class Execution(name : String, tasks : Seq[Record]) {

  def numTasks(taskType : TaskType) : Int = tasks.count(_.taskType == taskType)

  lazy val numMap : Int = numTasks(MapTask)

  lazy val numReduce : Int = numTasks(ReduceTask)

  val duration : Long

  lazy val locations : Set[String] = tasks.map(_.location).toSet

  def tasks(taskType : TaskType) : Seq[Record] = tasks.filter(_.taskType == taskType)

  def sum(taskType : TaskType) : Long = tasks(taskType).map(_.durationMSec).sum

  def max(taskType : TaskType) : Long = tasks(taskType).map(_.durationMSec).max

  def min(taskType : TaskType) : Long = tasks(taskType).map(_.durationMSec).min

  def avg(taskType : TaskType) : Long = sum(taskType) / numTasks(taskType)

  def numTasks(vertex : String) : Long

  def tasks(vertex : String) : Seq[Record]

  def sum(vertex : String) : Long

  def max(vertex : String) : Long

  def min(vertex : String) : Long

  def avg(vertex : String) : Long

  val vertices : List[String]

  val isNonTrivialDag : Boolean

}

object Execution {

  def apply (text : String, duration : Duration, shuffle : Shuffle, vertices : Vertices) : Execution = {
    val lines = text split "\n"
    new ExternalExecution(lines.head, duration, shuffle(vertices(lines map Record.apply)))
  }

}
