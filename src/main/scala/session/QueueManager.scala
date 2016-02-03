package session

/**
  * @author Alessandro
  */
case class QueueManager(queues: Map[String, Queue]) {

  def queue(name: String) = queues(name)

}

object QueueManager {

  def apply(session: Session, queue: (String, Double)*): QueueManager = {
    val totalNiceness = queue.map{ case (name, niceness) => niceness }.sum

    def ratio(niceness: Double) = niceness / totalNiceness
    val queues = queue.toMap

    val queueMap = session.threads groupBy { _.queueName }
    val actualQueues = queueMap map {
      case (name, threads) =>
        name -> Queue(name, ratio(queues(name)), 1, 1, threads.size)
    }
    QueueManager(actualQueues)
  }

}
