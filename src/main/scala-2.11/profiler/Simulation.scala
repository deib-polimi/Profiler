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

import java.io.File

import scala.io.Source

case class Simulation(executions: Seq[Execution], users: Int) {

  def avg(taskType: TaskType): Long = executions.map( _ sum taskType ).sum / executions.map( _ numTasks taskType ).sum
  def max(taskType: TaskType): Long = executions.map( _ max taskType ).max
  def min(taskType: TaskType): Long = executions.map( _ min taskType ).min

  def avg(vertex: String): Long = executions.map( _ sum vertex ).sum / executions.map( _ numTasks vertex ).sum
  def max(vertex: String): Long = executions.map( _ max vertex ).max
  def min(vertex: String): Long = executions.map( _ min vertex ).min

  lazy val avg: Long = executions.map( _.duration ).sum / executions.length
  lazy val max: Long = executions.map( _.duration ).max
  lazy val min: Long = executions.map( _.duration ).min

  def all(taskType: TaskType) = executions flatMap { _ tasks taskType }
  def all(vertex: String) = executions flatMap { _ tasks vertex }

  private def extractNumOf[N: Integral](groups: Map[N, Seq[Execution]]) = {
    val counts = groups map { case (key, list) => key -> list.size }
    val maxPair = counts maxBy { case (_, count) => count }
    maxPair._1
  }
  def numOf(taskType: TaskType): Int = extractNumOf(executions groupBy { _ numTasks taskType })
  def numOf(vertex: String): Long = extractNumOf(executions groupBy { _ numTasks vertex })

  val size: Int = executions.length

  lazy val vertices = executions.flatMap(_.vertices).toList.distinct sortBy {
    _.split(" ").last.toInt
  }
  lazy val isNonTrivialDag = executions exists { _.isNonTrivialDag }

  lazy val minShuffleBytes = executions.map( _.minShuffleBytes ).min
  lazy val maxShuffleBytes = executions.map( _.maxShuffleBytes ).max
  lazy val avgShuffleBytes = executions.map( _.sumShuffleBytes ).sum / executions.map( _.numShuffle ).sum

  def minShuffleBytes(vertex: String) = executions.map( _ minShuffleBytes vertex ).min
  def maxShuffleBytes(vertex: String) = executions.map( _ maxShuffleBytes vertex ).max
  def avgShuffleBytes(vertex: String) = executions.map( _ sumShuffleBytes vertex ).sum /
    executions.map( _ numTasks vertex ).sum

}

object Simulation {

  private val DEFAULT_ID = "default"

  def fromDir(dir: File): Map[String, Simulation] = {
    val durations = Duration(Source.fromFile(new File(dir, "appDuration.txt")).mkString)
    val vertices = Vertices(Source.fromFile(new File(dir, "vertexLtask.txt")).mkString)

    val idFile = new File(dir, "appId.txt")
    val someIds = if (idFile.canRead)
      Some(Identifiers(Source.fromFile(idFile).mkString)) else None

    val countsFile = new File(dir, "appUsers.txt")
    val someCounts = if (countsFile.canRead)
      Some(UserCount(Source.fromFile(countsFile).mkString)) else None

    val shuffleDurations = Source.fromFile(new File(dir, "shuffleStartEnd.txt")).mkString
    val shuffleBytes = Source.fromFile(new File(dir, "shuffleBytes.txt")).mkString
    val shuffle = Shuffle(shuffleDurations, shuffleBytes)

    val nodes = Nodes(Source.fromFile(new File(dir, "taskNode.txt")).mkString)

    val lines = Source.fromFile(new File(dir, "taskStartEnd.txt")).mkString
    val blocks = { lines split "\n\n" }.toSeq
    someIds match {
      case Some(identifiers) =>
        blocks groupBy {
          block =>
            val firstLine = { block split "\n" }.head split "\t"
            identifiers get firstLine.head
        } flatMap {
          case (Some(id), theseBlocks) =>
            someCounts match {
              case Some(counts) =>
                val users = counts get id getOrElse 1
                Some(id -> Simulation(theseBlocks, durations, shuffle, vertices, nodes, users))
              case None =>
                Some(id -> Simulation(theseBlocks, durations, shuffle, vertices, nodes, 1))
            }
          case (None, _) => None
        }
      case None =>
        Map(DEFAULT_ID -> Simulation(blocks, durations, shuffle, vertices, nodes, 1))
    }
  }

  private def apply(blocks: Seq[String], duration: Duration, shuffle: Shuffle,
                    vertices: Vertices, nodes: Nodes, users: Int): Simulation = {
    val executions = blocks map { Execution(_, duration, shuffle, vertices, nodes)
    } filter { duration contains _.taskId }
    executions foreach {
      x =>
        Console.err println
          s"Map tasks: ${ x numTasks MapTask } Reduce tasks: ${ x numTasks ReduceTask }"
    }
    Simulation(executions, users)
  }
}
