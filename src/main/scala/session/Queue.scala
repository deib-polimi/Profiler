package session

/**
  * @author Alessandro
  */
case class Queue(name : String, ratio : Double, nMap : Long, nReduce : Long, users : Int) {

  lazy val mapRatio = nMap.toDouble / (nMap + nReduce)

  lazy val reduceRatio = nReduce.toDouble / (nMap + nReduce)

}
