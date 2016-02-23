package session

import java.io.File

import scala.io.Source

/**
  * @author Alessandro
  */
case class Session(threads: List[Thread]) {

  lazy val avgByQuery: Map[String, Long] = threads groupBy { _.query } map {
    case (key, list) =>
      val durations = list flatMap { _.executions } map { _.duration }
      key -> durations.sum / durations.size
  }

  def validate(queueManager: QueueManager, numCores: Int): Map[String, Double] =
    threads map { x => x.query -> x.validate(queueManager, numCores) } groupBy
      { _._1 } map { case (key, couples) => key -> couples.map(_._2).sum / couples.size }

  def validateUpper(queueManager: QueueManager, numCores: Int): Map[String, Double] =
    threads map { x => x.query -> x.validateUpper(queueManager, numCores) } groupBy
      { _._1 } map { case (key, couples) => key -> couples.map(_._2).sum / couples.size }

  def validateWith(deadline: Long): Map[String, Double] =
    threads map { x => x.query -> x.validateWith(deadline) } groupBy
      { _._1 } map { case (key, couples) => key -> couples.map(_._2).sum / couples.size }

}

object Session {

  def apply(dir: File, profileDir: File): Session = {
    val files = dir.listFiles.toList
    val threads = files map { x => Thread(Source.fromFile(x).mkString, x.getName, profileDir) }
    Session(threads)
  }

  def mainEntryPoint(inputDir: File, profilesDir: File, nCores: Int,
                     deadline: Int, queues: (String, Double)*): Unit = {
    val session = Session(inputDir, profilesDir)
    val manager = QueueManager(session, queues:_*)

    println("Users:")
    manager.queues foreach { case (name, queue) => println(s"$name: ${queue.users}") }
    println("Measured times:")
    session.avgByQuery foreach println
    println("Average:")
    session validate (manager, nCores) foreach println
    println("Upper:")
    session validateUpper (manager, nCores) foreach println
    println("Deadline:")
    session validateWith deadline foreach println
  }

}
