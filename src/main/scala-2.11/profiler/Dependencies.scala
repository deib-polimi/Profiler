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

import scala.util.parsing.json.JSON

class Dependencies(dependencies: Map[String, Map[String, List[String]]]) {
  def apply(query: String): Map[String, List[String]] = dependencies(query)
}

object Dependencies {
  def apply(jsonContent: String): Dependencies = {
    val maybeData = JSON parseFull jsonContent
    maybeData match {
      case Some(data) => new Dependencies(data.asInstanceOf[Map[String, Map[String, List[String]]]])
      case None => throw new RuntimeException("malformed JSON file")
    }
  }
}
