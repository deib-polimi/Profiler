/* Copyright 2015-2016 Alessandro Maria Rizzi
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package session

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
