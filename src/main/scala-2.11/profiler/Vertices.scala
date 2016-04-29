/* Copyright 2015-2016 Eugenio Gianniti
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

class Vertices(assignments: Map[String, String]) {

  private def apply(name: String) = assignments(parseIdentifier(name))

  def get(name: String) = assignments get parseIdentifier(name)

  def contains(name: String) = assignments contains parseIdentifier(name)

  private def parseIdentifier(name: String) = { name split "_" }.tail mkString "_"

  private def inferType(name: String) = get(name) match {
    case Some(vertex) if vertex startsWith "Map" => MapTask
    case Some(vertex) if vertex startsWith "Reduce" => ReduceTask
    case Some(vertex) => throw new RuntimeException(s"unrecognized vertex type: $vertex")
    case None =>
      val missing = parseIdentifier(name)
      throw new RuntimeException(s"missing vertex assignment for task: $missing")
  }

  def apply(records: Seq[Record]): Seq[Record] = records map {
    record =>
      val taskType = inferType(record.name)
      val vertex = apply(record.name)
      record copy (taskType = taskType, vertex = vertex)
  }

}

object Vertices {

  def apply(text: String): Vertices = {
    def parseLine(line: String) = {
      val vertex :: attempts = { line split "\t" }.toList
      attempts map {
        attempt =>
          val identifier = { attempt split "_" }.tail mkString "_"
          identifier -> vertex
      }
    }
    val couples = text split "\n" flatMap parseLine
    new Vertices(couples.toMap)
  }
}
