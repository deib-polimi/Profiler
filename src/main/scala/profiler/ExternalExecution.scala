package profiler

class ExternalExecution(override val name : String, external : Duration,
                        tasks : Array[Record])
  extends Execution(name, tasks) {

  override def duration = {external get tasks.head.name}.get

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

  override val vertices = tasks.map(_.vertex).toList.distinct

  override val isNonTrivialDag = {vertices filterNot {_ startsWith "Shuffle"} lengthCompare 2} > 0

}
