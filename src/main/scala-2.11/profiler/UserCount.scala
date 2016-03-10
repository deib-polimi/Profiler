/* Copyright 2016 Eugenio Gianniti
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

class UserCount(counts: Map[String, Int]) {

  def get(query: String) = counts get query

  def contains(query: String) = counts contains query

}

object UserCount {

  def apply(text: String): UserCount = {

    def parseEntry(line: String): List[(String, Int)] = {
      val fields = { line split "\t" }.toList
      val externalId = fields.head
      fields.tail map { field => externalId -> field.toInt }
    }

    val lines = { text split "\n" }.toList
    val counts = lines flatMap parseEntry
    new UserCount(counts.toMap)
  }

}
