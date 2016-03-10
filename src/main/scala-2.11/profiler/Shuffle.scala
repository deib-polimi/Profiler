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
package profiler

class Shuffle(shuffles: Map[String, Record], bytes: Map[String, Long]) {

  def apply(data: Seq[Record]): Seq[Record] = {
    val maps = data filter { _.taskType == MapTask }
    val reduces = data filter { _.taskType == ReduceTask } map {
      task => task - shuffles(task.name)
    }
    val theseShuffles = reduces flatMap {
      task =>
        val newVertex = s"Shuffle ${task.vertex.split(" ").last}"
        bytes get task.name match {
          case Some(value) =>
            Some(shuffles(task.name) copy (taskType = ShuffleTask, vertex = newVertex, bytes = value))
          case None => None
        }
    }
    maps ++ reduces ++ theseShuffles
  }

}

object Shuffle {

  def apply(durationsContent: String, bytesContent: String): Shuffle = {
    val entries = durationsContent split "\n\n" flatMap { _ split "\n" } map
      Record.apply map { x => x.name -> x }
    val bytes = bytesContent split "\n\n" flatMap { _ split "\n" } map {
      line => { line split "\t" }.toList
    } flatMap {
      case name :: data :: Nil => Some(name -> data.toLong)
      case _ => None
    }
    new Shuffle(entries.toMap, bytes.toMap)
  }

}
