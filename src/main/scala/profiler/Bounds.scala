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

  def slots : Double = numCores.toDouble

  def mapRatio = 1.0d

  def reduceRatio = 1.0d

  def mapSlots = mapRatio * slots

  def reduceSlots = reduceRatio * slots

  def avgMap = simulation.avg(MapTask)

  def avgReduce = simulation.avg(ReduceTask)

  def avgShuffle = simulation.avg(ShuffleTask)

  def maxMap = simulation.max(MapTask)

  def maxReduce = simulation.max(ReduceTask)

  def maxShuffle = simulation.max(ShuffleTask)

  def numMap = simulation.numOf(MapTask)

  def numReduce = simulation.numOf(ReduceTask)

  def upperBound = (avgMap * numMap - 2 * maxMap) / mapSlots +
    ((avgReduce + avgShuffle) * numReduce - 2 * (maxReduce + maxShuffle)) / reduceSlots +
    2 * (maxMap + maxReduce + maxShuffle)

  def lowerBound = (avgMap * numMap + (avgReduce + avgShuffle) * numReduce) / slots

  def avg = (upperBound + lowerBound) / 2

}
