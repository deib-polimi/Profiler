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

case class Duration(durations: Map[String, Long]) {

  private def get(name: String) = durations get name

  def contains(input: String) = durations contains parseIdentifier(input)

  private def parseIdentifier(name: String) = name split "_" slice (1, 3) mkString "_"

  def obtainTotalDuration(names: Seq[String]) = {
    val ids = names.map(parseIdentifier).distinct
    val theseDurations = ids flatMap get
    theseDurations.sum
  }

}

object Duration {

  def apply(text: String): Duration = {
    def parseEntry(text: String): Option[(String, Long)] = {
      val fields = text split "\t"
      val duration = fields(0).toLong
      if (duration < 0) None
      else Some(fields(1).split("_").tail.mkString("_") -> duration)
    }
    val lines = text split "\n"
    Duration(lines.flatMap(parseEntry).toMap)
  }

}
