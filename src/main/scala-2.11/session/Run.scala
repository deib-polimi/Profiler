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

import java.text.SimpleDateFormat
import java.util.Locale

case class Run(name: String, start: Long, duration: Long)

object Run {

  private def parseData(input: String) =
    new SimpleDateFormat("d HH:mm:ss.SSS", Locale.ENGLISH).parse(input).getTime

  def apply(text: String): Run = {
    val fields = text split "," map { _.trim }
    val start = parseData(fields(1))
    val stop = parseData(fields(2))
    Run(fields.head, start, stop - start)
  }

}
