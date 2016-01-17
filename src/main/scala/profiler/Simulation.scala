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

  def avg(taskType : TaskType) : Long = executions.map(_.sum(taskType)).sum / executions.map(_.numTasks(taskType).toLong).sum

  def max(taskType : TaskType) : Long = executions.map(_.max(taskType)).max

  def min(taskType : TaskType) : Long = executions.map(_.min(taskType)).min

  def avg(vertex : String) : Long = executions.map(_.sum(vertex)).sum / executions.map(_.numTasks(vertex)).sum

  def max(vertex : String) : Long = executions.map(_.max(vertex)).max

  def min(vertex : String) : Long = executions.map(_.min(vertex)).min

  lazy val avg : Long = executions.map(_.duration).sum / executions.length

  lazy val max : Long = executions.map(_.duration).max

  lazy val min : Long = executions.map(_.duration).min

  def all(taskType : TaskType) = executions.flatMap(_.tasks(taskType)).sortBy(_.durationMSec)

  def all(vertex : String) = executions.flatMap(_.tasks(vertex))

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
    val isInWantedRange : (Int) => Boolean = Range(start, start + size).contains
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

  def numOf (vertex : String): Long = {
    val num = executions.map(_.numTasks(vertex)).toSet
    num.max
  }

  def validate(bounds : Bounds) = executions.map(x => bounds.error(x.duration)*100).sortBy(x => x)

  def validateMore(bounds : Bounds) =
    executions.map(x => (bounds.error(x.duration)*100, x.locations.size) ).sortBy(x => x._1)

  def over (bounds : Bounds) : Int = executions.count(_.duration > bounds.upperBound)

  def under (bounds : Bounds) : Int = executions.count(_.duration < bounds.lowerBound)

  val size : Int = executions.length

  lazy val vertices = executions.flatMap(_.vertices).toList.distinct sortBy {_.split(" ").last.toInt}

  lazy val isNonTrivialDag = executions exists {_.isNonTrivialDag}

}

object Simulation {

  def fromDir (dir : File) : Simulation = {
    val dataDir = new File (dir, "data")
    val durations = Duration (Source.fromFile (new File (dataDir, "appDuration.txt")).mkString)
    val lines = Source.fromFile (new File (dataDir, "taskDurationLO.txt")).mkString
    val shuffle = Shuffle (Source.fromFile (new File (dataDir, "shuffleDurationLO.txt")).mkString)
    val vertices = Vertices(Source.fromFile(new File(dataDir, "vertexLtask.txt")).mkString)
    Simulation (lines, durations, shuffle, vertices)
  }

  def apply (text : String, duration : Duration, shuffle : Shuffle, vertices : Vertices) : Simulation = {
    val executions = text.split("\n\n").map(Execution(_, duration, shuffle, vertices)).
      filter(x => duration.contains(x.tasks.head.name))
    executions.foreach (x => Console.err.println ("Map tasks: " + x.tasks (MapTask).length +
      " Reduce tasks: " + x.tasks (ReduceTask).length))
    Simulation (executions)
  }
}
