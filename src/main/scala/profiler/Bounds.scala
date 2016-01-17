/**
  *
  */
package profiler

/**
  * @author Alessandro
  *
  */
case class Bounds(simulation : Simulation, numCores : Int) {

  def errorUpper(duration : Long) : Double = {
    val delta = upperBound - duration
    delta.toDouble / duration.toDouble
  }

  def error(duration : Long) : Double = {
    val delta = avg - duration
    delta.toDouble / duration.toDouble
  }

  val slots : Double = numCores.toDouble

  val mapRatio = 1.0d

  val reduceRatio = 1.0d

  val mapSlots = mapRatio * slots

  val reduceSlots = reduceRatio * slots

  lazy val avgMap = simulation.avg(MapTask)

  lazy val avgReduce = simulation.avg(ReduceTask)

  lazy val avgShuffle = simulation.avg(ShuffleTask)

  lazy val maxMap = simulation.max(MapTask)

  lazy val maxReduce = simulation.max(ReduceTask)

  lazy val maxShuffle = simulation.max(ShuffleTask)

  lazy val numMap = simulation.numOf(MapTask)

  lazy val numReduce = simulation.numOf(ReduceTask)

  lazy val upperBound = (avgMap * numMap - 2 * maxMap) / mapSlots +
    ((avgReduce + avgShuffle) * numReduce - 2 * (maxReduce + maxShuffle)) / reduceSlots +
    2 * (maxMap + maxReduce + maxShuffle)

  lazy val lowerBound = (avgMap * numMap + (avgReduce + avgShuffle) * numReduce) / slots

  lazy val avg = (upperBound + lowerBound) / 2

}
