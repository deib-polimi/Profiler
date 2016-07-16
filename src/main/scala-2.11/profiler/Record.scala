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

import java.text.SimpleDateFormat
import java.util.Locale

import scala.annotation.switch

case class Record(name: String, durationMSec: Long, startMSec: Long = -1,
                  stopMSec: Long = -1, taskType: TaskType,
                  location: String = "UNKNOWN", vertex: String = "UNKNOWN",
                  bytes: Long = -1, node: String = "UNKNOWN") {

  /**
    * This method is meant to subtract the shuffle subtask from the beginning of a reducer
    * @param shuffle the shuffle subtask
    * @return a clean reducer Record without shuffle subtask
    * @throws RuntimeException if the shuffle subtask does not overlap or is too long
    */
  def cutShuffleFromHead(shuffle: Record): Record = if (stopMSec < shuffle.startMSec)
    throw new RuntimeException(s"error: shuffle subtask ${shuffle.name} does not overlap reducer")
  else if (stopMSec < shuffle.stopMSec)
    throw new RuntimeException(s"error: shuffle subtask ${shuffle.name} is longer than reducer")
  else copy(durationMSec = stopMSec - shuffle.stopMSec, startMSec = shuffle.stopMSec)

  def cutFrontOverlap(previousCompletion: Long): Record = previousCompletion match {
    case time if stopMSec < time =>
      Console.err println s"warning: task $name completely overlaps dependency"
      copy(startMSec = -2, stopMSec = -2, durationMSec = -2)
    case time if startMSec < time =>
      copy(startMSec = time, durationMSec = stopMSec - time)
    case _ => this
  }

}

object Record {

  private def getType(input: String): TaskType =
    if (input.split("_")(4).toInt == 0) MapTask
    else ReduceTask

  private def parseDate(input: String) =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH).parse(input).getTime

  def apply(text: String): Record = {
    val fields = text split "\t"
    (fields.size: @switch) match {
      case 2 =>
        Record(name = fields(0), durationMSec = fields(1).toLong, taskType = getType(fields(0)))
      case 3 =>
        val start = fields(1).toLong
        val stop = fields(2).toLong
        Record(name = fields(0), durationMSec = stop - start, startMSec = start,
          stopMSec = stop, taskType = getType(fields(0)))
      case 5 =>
        Record(fields(0), fields(1).toInt, parseDate(fields(2)), parseDate(fields(3)),
          getType(fields(0)), fields(4))
      case _ => throw new RuntimeException(s"wrong entry: $text")
    }
  }

}
