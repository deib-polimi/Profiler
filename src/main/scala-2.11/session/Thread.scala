package session

import java.io.File

import profiler.Simulation

/**
  * @author Alessandro
  */
case class Thread(user: String, query: String, queueName: String, id: String, executions: Seq[Run], profile: Simulation) {

  lazy val avg : Long = executions.map(_.duration).sum / executions.size

  val fullId = s"$user $query $id"

  def validate(queueManager: QueueManager, numCores: Int): Double = {
    val bounds = new SessionBounds(queueManager queue queueName, profile, numCores)
    bounds error avg
  }

  def validateUpper(queueManager: QueueManager, numCores: Int): Double = {
    val bounds = new SessionBounds(queueManager queue queueName, profile, numCores)
    bounds errorUpper avg
  }

  def validateWith(deadline: Long) = (deadline.toDouble - avg) / avg

}

object Thread {

  private def processFilename(filename: String): List[String] = {
    val name = filename dropRight 4 split "_"
    name.toList
  }

  def apply(text: String, filename: String, profileDir: File): Thread = {
    val runs = { text split "\n" map Run.apply }.toSeq
    processFilename(filename) match {
      case List(user, query, queue, id) =>
        val profile = Profile (new File(profileDir, query))
        Thread(user, query, queue, id, runs, profile)
    }
  }

}
