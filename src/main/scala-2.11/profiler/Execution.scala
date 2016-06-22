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
package profiler

class Execution(name: String, tasks: Seq[Record], allDurations: Duration) {

  lazy val numMap: Int = numTasks(MapTask)
  lazy val numReduce: Int = numTasks(ReduceTask)
  lazy val numShuffle: Int = numTasks(ShuffleTask)

  private lazy val taskNames: Seq[String] = tasks map { _.name }
  lazy val taskId: String = taskNames.head
  lazy val duration: Long = allDurations obtainTotalDuration taskNames

  lazy val locations: Set[String] = tasks.map(_.location).toSet

  def tasks(taskType: TaskType): Seq[Record] = tasks filter (_.taskType == taskType)
  def numTasks(taskType: TaskType): Int = tasks(taskType).length
  def sum(taskType: TaskType): Long = tasks(taskType).map(_.durationMSec).sum
  def max(taskType: TaskType): Long = tasks(taskType).map(_.durationMSec).max
  def min(taskType: TaskType): Long = tasks(taskType).map(_.durationMSec).min
  def avg(taskType: TaskType): Long = sum(taskType) / numTasks(taskType)

  def tasks(vertex: String): Seq[Record] = tasks filter { _.vertex == vertex }
  def numTasks(vertex: String): Long = tasks(vertex).length
  def sum(vertex: String): Long = tasks(vertex).map(_.durationMSec).sum
  def max(vertex: String): Long = tasks(vertex).map(_.durationMSec).max
  def min(vertex: String): Long = tasks(vertex).map(_.durationMSec).min
  def avg(vertex: String): Long = sum(vertex) / numTasks(vertex)

  lazy val vertices: List[String] = tasks.map(_.vertex).toList.distinct sortBy { _.split(" ").last.toInt }
  lazy val isNonTrivialDag: Boolean = {
    // lengthCompare is O(2) instead of O(length)
    val moreThanTwo = vertices filterNot { _ startsWith "Shuffle" } lengthCompare 2
    moreThanTwo > 0
  }

  lazy val shuffleBytes: Seq[Long] = tasks(ShuffleTask) map { _.bytes }
  lazy val sumShuffleBytes: Long = shuffleBytes.sum
  lazy val maxShuffleBytes: Long = shuffleBytes.max
  lazy val minShuffleBytes: Long = shuffleBytes.min
  lazy val avgShuffleBytes: Long = sumShuffleBytes / numShuffle

  def shuffleBytes(vertex: String): Seq[Long] = tasks(vertex) map { _.bytes }
  def sumShuffleBytes(vertex: String): Long = shuffleBytes(vertex).sum
  def maxShuffleBytes(vertex: String): Long = shuffleBytes(vertex).max
  def minShuffleBytes(vertex: String): Long = shuffleBytes(vertex).min
  def avgShuffleBytes(vertex: String): Long = sumShuffleBytes(vertex) / numTasks(vertex)

  lazy val nodes: Seq[String] = tasks.map( _.node ).toList.distinct.sorted
  private lazy val tasksByNodes: Map[String, Seq[Record]] = tasks groupBy { _.node }

  def tasks(vertex: String, node: String): Seq[Record] = tasksByNodes getOrElse
    (node, Seq[Record]()) filter { _.vertex == vertex }
  def numTasks(vertex: String, node: String): Long = tasks(vertex, node).length
  def sum(vertex: String, node: String): Option[Long] = try {
    Some(tasks(vertex, node).map( _.durationMSec ).sum)
  } catch {
    case e: UnsupportedOperationException => None
  }
  def max(vertex: String, node: String): Option[Long] = try {
    Some(tasks(vertex, node).map( _.durationMSec ).max)
  } catch {
    case e: UnsupportedOperationException => None
  }
  def min(vertex: String, node: String): Option[Long] = try {
    Some(tasks(vertex, node).map( _.durationMSec ).min)
  } catch {
    case e: UnsupportedOperationException => None
  }
  def avg(vertex: String, node: String): Option[Long] = sum(vertex, node) map { _ / numTasks(vertex, node) }

  def cleanOverlaps(dependencies: Map[String, List[String]]): Execution = {
    val groups = tasks groupBy { _.vertex }
    val nextTasks = groups flatMap {
      case (vertex, theseTasks) =>
        val actualVertexName = vertex replace ("Shuffle", "Reducer")
        dependencies get actualVertexName map {
          list =>
            val predecessorCompletions = list flatMap groups.apply map { _.stopMSec }
            val lastCompletion = predecessorCompletions.max
            theseTasks map { _ cutFrontOverlap lastCompletion }
        } getOrElse theseTasks
    }
    new Execution(name, nextTasks.toSeq, allDurations)
  }

}

object Execution {

  def apply(text: String, duration: Duration, shuffle: Shuffle, vertices: Vertices,
            nodes: Nodes): Execution = {
    val lines = text split "\n"
    new Execution(lines.head, nodes(shuffle(vertices(lines map Record.apply))), duration)
  }

}
