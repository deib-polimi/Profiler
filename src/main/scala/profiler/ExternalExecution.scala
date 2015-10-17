package profiler

class ExternalExecution(override val name : String, external : Duration,
                        tasks : Array[Record], mVertices : Vertices)
  extends Execution(name, tasks) {

  override def duration = {external get tasks.head.name}.get

  override def tasks(vertex : String) = tasks filter {
    mVertices get _.name match {
      case Some(`vertex`) => true
      case _ => false
    }
  }

  override def numTasks(vertex : String) = tasks(vertex).length

  override def sum(vertex : String) = tasks(vertex).map(_.durationMSec).sum

  override def max(vertex : String) = tasks(vertex).map(_.durationMSec).max

  override def min(vertex : String) = tasks(vertex).map(_.durationMSec).min

  override def avg(vertex : String) = sum(vertex) / numTasks(vertex)

  override val vertices = mVertices.distinctVertices

  override val isNonTrivialDag = mVertices.isNonTrivialDag

}
