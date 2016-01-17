package profiler

class ExternalExecution(override val name : String, external : Duration,
                        tasks : Seq[Record])
  extends Execution(name, tasks) {

  override lazy val duration = {
    val jobs = tasks.map(x => (external parseIdentifier x.name) -> x.name).toMap
    val durations = jobs flatMap {case (id, task) => external get task}
    durations.sum
  }

  override def tasks(vertex : String) = tasks filter {
    _.vertex match {
      case `vertex` => true
      case _ => false
    }
  }

  override def numTasks(vertex : String) = tasks(vertex).length

  override def sum(vertex : String) = tasks(vertex).map(_.durationMSec).sum

  override def max(vertex : String) = tasks(vertex).map(_.durationMSec).max

  override def min(vertex : String) = tasks(vertex).map(_.durationMSec).min

  override def avg(vertex : String) = sum(vertex) / numTasks(vertex)

  override lazy val vertices = tasks.map(_.vertex).toList.distinct

  override lazy val isNonTrivialDag = {vertices filterNot {_ startsWith "Shuffle"} lengthCompare 2} > 0

}
