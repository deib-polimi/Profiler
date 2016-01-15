package session

import java.io.File

import scala.io.Source

/**
 * @author Alessandro
 */
case class Session(threads : List[Thread]) {

  def avgByQuery : Map[String, Long] = threads groupBy {_.query} map {
    case (key, list) =>
      val durations = list flatMap {_.sequence} map {_.duration}
      key -> durations.sum / durations.size
  }

  def validate (queueManager : QueueManager, numCores : Int): Map[String, Double] =
    threads.map(x => x.query -> x.validate(queueManager, numCores)).groupBy(_._1).
      map(x => x._1 -> x._2.map(_._2).sum/x._2.size)

  def validateUpper(queueManager : QueueManager, numCores : Int): Map[String, Double] =
    threads.map(x => x.query -> x.validateUpper(queueManager, numCores)).groupBy(_._1).
      map(x => x._1 -> x._2.map(_._2).sum/x._2.size)

  def validateWith(deadline : Long): Map[String, Double] =
    threads.map(x => x.query -> x.validateWith(deadline)).groupBy(_._1).
      map(x => x._1 -> x._2.map(_._2).sum/x._2.size)

}

object Session {

  def apply (dir : File, profileDir : File) : Session =  {
    val files = dir.listFiles ().toList
    val threads = files.map (x => Thread (Source.fromFile (x).mkString, x.getName, profileDir))
    Session(threads)
  }

  def mainEntryPoint (inputDir : File, profilesDir : File, nCores : Int,
                      deadline : Int, queues : (String, Double)*) : Unit = {
    val session = Session (inputDir, profilesDir)
    val manager = QueueManager (session, queues:_*)

    println("Users:")
    manager.queues.foreach(queue => println(s"${queue._1}: ${queue._2.users}"))
    println("Measured times:")
    session.avgByQuery foreach println
    println("Average:")
    session.validate(manager, nCores).foreach(x => println(x))
    println("Upper:")
    session.validateUpper(manager, nCores).foreach(x => println(x))
    println("Deadline:")
    session.validateWith(deadline).foreach(x => println(x))
  }

}
