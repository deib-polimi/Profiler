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

import java.io.File

import profiler.Simulation

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
