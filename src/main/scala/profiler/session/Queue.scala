package profiler.session

/**
 * @author Alessandro
 */
case class Queue(name : String, ratio : Double, nMap : Long, nReduce : Long, users : Int) {

  def mapRatio = nMap.toDouble / (nMap + nReduce)

  def reduceRatio = nReduce.toDouble / (nMap + nReduce)

};
