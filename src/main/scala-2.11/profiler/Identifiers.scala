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

class Identifiers(ids: Map[String, String]) {

  def get(input: String) = ids get parseIdentifier(input)

  def contains(input: String) = ids contains parseIdentifier(input)

  private def parseIdentifier(name: String) = name split "_" slice(1, 3) mkString "_"

}

object Identifiers {

  def apply(text: String): Identifiers = {

    def parseEntry(line: String): List[(String, String)] = {
      val fields = { line split "\t" }.toList
      val externalId = fields.head
      fields.tail map { field => field.split("_").tail.mkString("_") -> externalId }
    }

    val lines = { text split "\n" }.toList
    val ids = lines flatMap parseEntry
    new Identifiers(ids.toMap)
  }

}
