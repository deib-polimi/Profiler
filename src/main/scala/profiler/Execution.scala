/**
 *
 */
package profiler

/**
 * @author Alessandro
 *
 */
abstract case class Execution(name : String, tasks : Array[Record]) {

  def numTasks(taskType : TaskType) : Int = tasks.count(_.taskType == taskType)

  def numMap : Int = numTasks(MapTask)

  def numReduce : Int = numTasks(ReduceTask)

  def duration : Long

  def locations : Set[String] = tasks.map(_.location).toSet

  def tasks(taskType : TaskType) : Array[Record] = tasks.filter(_.taskType == taskType)

  def sum(taskType : TaskType) : Long = tasks(taskType).map(_.durationMSec).sum

  def max(taskType : TaskType) : Long = tasks(taskType).map(_.durationMSec).max

  def min(taskType : TaskType) : Long = tasks(taskType).map(_.durationMSec).min

  def avg(taskType : TaskType) : Long = sum(taskType) / numTasks(taskType)

}

object Execution {

  def apply (text : String, duration : Duration, shuffle : Shuffle, vertices : Vertices) : Execution = {
    val lines = text.split("\n")
    new ExternalExecution(lines.head, duration, shuffle(vertices(lines map Record.apply)))
  }

}
