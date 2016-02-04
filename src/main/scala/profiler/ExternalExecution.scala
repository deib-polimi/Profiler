package profiler

class ExternalExecution(override val name: String, external: Duration, tasks: Seq[Record])
  extends Execution(name, tasks) {

  override lazy val duration = {
    val names = tasks map { _.name }
    external obtainTotalDuration names
  }

  override def tasks(vertex: String) = tasks filter {
    _.vertex match {
      case `vertex` => true
      case _ => false
    }
  }
  override def numTasks(vertex: String) = tasks(vertex).length
  override def sum(vertex: String) = tasks(vertex).map(_.durationMSec).sum
  override def max(vertex: String) = tasks(vertex).map(_.durationMSec).max
  override def min(vertex: String) = tasks(vertex).map(_.durationMSec).min
  override def avg(vertex: String) = sum(vertex) / numTasks(vertex)

  override lazy val vertices = tasks.map(_.vertex).toList.distinct
  override lazy val isNonTrivialDag = {
    // lengthCompare is O(2) instead of O(length)
    val moreThanTwo = vertices filterNot { _ startsWith "Shuffle" } lengthCompare 2
    moreThanTwo > 0
  }

  override def shuffleBytes(vertex: String): Seq[Long] = tasks(vertex) map { _.bytes }
  override def sumShuffleBytes(vertex: String): Long = shuffleBytes(vertex).sum
  override def maxShuffleBytes(vertex: String): Long = shuffleBytes(vertex).max
  override def minShuffleBytes(vertex: String): Long = shuffleBytes(vertex).min
  override def avgShuffleBytes(vertex: String): Long = sumShuffleBytes(vertex) / numTasks(vertex)

}
