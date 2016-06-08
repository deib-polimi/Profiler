/* Copyright 2015 Alessandro Maria Rizzi
 * Copyright 2016 Eugenio Gianniti
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

import scala.io.Source

case class Session(threads: List[Thread]) {

  lazy val avgByQuery: Map[String, Long] = threads groupBy { _.query } map {
    case (key, list) =>
      val durations = list flatMap { _.executions } map { _.duration }
      key -> durations.sum / durations.size
  }

  def validate(queueManager: QueueManager, numContainers: Int): Map[String, Double] =
    threads map { x => x.query -> x.validate(queueManager, numContainers) } groupBy
      { _._1 } map { case (key, couples) => key -> couples.map(_._2).sum / couples.size }

  def validateUpper(queueManager: QueueManager, numContainers: Int): Map[String, Double] =
    threads map { x => x.query -> x.validateUpper(queueManager, numContainers) } groupBy
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

  def mainEntryPoint(inputDir: File, profilesDir: File, nContainers: Int,
                     deadline: Int, queues: (String, Double)*): Unit = {
    val session = Session(inputDir, profilesDir)
    val manager = QueueManager(session, queues:_*)

    println("Users:")
    manager.queues foreach { case (name, queue) => println(s"$name: ${queue.users}") }
    println("Measured times:")
    session.avgByQuery foreach println
    println("Average:")
    session validate (manager, nContainers) foreach println
    println("Upper:")
    session validateUpper (manager, nContainers) foreach println
    println("Deadline:")
    session validateWith deadline foreach println
  }

}
