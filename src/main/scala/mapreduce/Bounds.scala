/**
 *
 */
package mapreduce

/**
 * @author Alessandro
 *
 */
case class Bounds(simulation : Simulation, numCores : Int) {  
  
  def errorUpper(duration : Long) : Double = {
    val delta = upperBound - duration;
    delta.toDouble / duration.toDouble;
  }  
  
  def error(duration : Long) : Double = {
    val delta = avg - duration;
    delta.toDouble / duration.toDouble;
  } 
  
  def slots : Double = numCores.toDouble;
  
  def mapRatio = 1.0d;
  
  def reduceRatio = 1.0d;
  
  def mapSlots = mapRatio * slots;
  
  def reduceSlots = reduceRatio * slots;
  
  def avgMap = simulation.avg(MAP);
  
  def avgReduce = simulation.avg(REDUCE);
  
  def avgShuffle = simulation.avg(SHUFFLE);
  
  def maxMap = simulation.max(MAP);
  
  def maxReduce = simulation.max(REDUCE);
  
  def maxShuffle = simulation.max(SHUFFLE);
  
  def numMap = simulation.numOf(MAP);
  
  def numReduce = simulation.numOf(REDUCE);
  
  def upperBound = (avgMap * numMap - 2 * maxMap) / mapSlots + ((avgReduce + avgShuffle) * numReduce - 2 * (maxReduce+maxShuffle)) / reduceSlots + 2*(maxMap + maxReduce + maxShuffle);
  
  def lowerBound = (avgMap * numMap + (avgReduce + avgShuffle) * numReduce) / slots;// - avgShuffle;
  
  def avg = (upperBound + lowerBound) / 2;  

}