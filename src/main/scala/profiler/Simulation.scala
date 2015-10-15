/**
 *
 */
package profiler

import java.io.File

import scala.io.Source

/**
 * @author Alessandro
 *
 */
case class Simulation(executions : Array[Execution]) {

  private def sum(l : Array[Long]) : Long = l.sum

  def avg(taskType : TaskType) : Long = sum(executions.map(_.sum(taskType))) / sum(executions.map(_.numTasks(taskType).toLong))

  def max(taskType : TaskType) : Long = executions.map(_.max(taskType)).max

  def min(taskType : TaskType) : Long = executions.map(_.min(taskType)).min

  def avg : Long = sum (executions map (_.duration)) / executions.length

  def max : Long = executions.map(_.duration).max

  def min : Long = executions.map(_.duration).min

  def all(taskType : TaskType) = executions.flatMap(_.tasks(taskType)).sortBy(_.durationMSec)

  def kFold(subdivision : Int) = {
    val size = executions.length/subdivision
    val range = Range(0, executions.length, size)
    range.map(x => setRange(x, size)).filterNot(x => x._1.executions.length < size)
  }

  private def filter(selected : Seq[Int]) = {
    val slice = for(i <- selected) yield executions(i)
    Simulation(slice.toArray)
  }

  private def setRange(start : Int, size : Int) : (Simulation, Simulation) = {
    val all = executions.indices
    val isInWantedRange = Range(start, start + size) contains _
    val selected = all filter isInWantedRange
    val unselected = all filterNot isInWantedRange
    (filter(selected), filter(unselected))
  }

  def range(min : Int, max : Int) : Simulation = {
    val slice = for(i <- Range(min, max)) yield executions(i)
    Simulation (slice.toArray)
  }

  def numOf (taskType : TaskType): Int = {
    val num = executions.map(_.numTasks(taskType)).toSet
    num.max
  }

  def validate(bounds : Bounds) = executions.map(x => bounds.error(x.duration)*100).sortBy(x => x)

  def validateMore(bounds : Bounds) =
    executions.map(x => (bounds.error(x.duration)*100, x.locations.size) ).sortBy(x => x._1)

  def over (bounds : Bounds) : Int = executions.count(_.duration > bounds.upperBound)

  def under (bounds : Bounds) : Int = executions.count(_.duration < bounds.lowerBound)

  def size : Int = executions.length

}

object Simulation {

  def fromDir (dir : File) : Simulation = {
    val dataDir = new File (dir, "data")
    val durations = Duration (Source.fromFile (new File (dataDir,
      "appDuration.txt")).mkString)
    val lines = Source.fromFile (new File (dataDir,
      "taskDurationLO.txt")).mkString
    val shuffle = Shuffle (Source.fromFile (new File (dataDir,
      "shuffleDurationLO.txt")).mkString)
    Simulation (lines, durations, shuffle)
  }

  def apply (text : String, duration : Duration, shuffle : Shuffle) : Simulation = {
    val executions = text.split("\n\n").map(Execution(_, duration, shuffle)).
      filter(x => duration.contains(x.tasks.head.name))
    executions.foreach (x => Console.err.println ("Map tasks: " + x.tasks (MapTask).length +
      " Reduce tasks: " + x.tasks (ReduceTask).length))
    Simulation (executions)
  }
}
