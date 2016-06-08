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

case class Bounds(simulation: Simulation, numContainers: Int) {

  def errorUpper(duration: Long): Double = {
    val delta = upperBound - duration
    delta.toDouble / duration.toDouble
  }

  def error(duration: Long): Double = {
    val delta = avg - duration
    delta.toDouble / duration.toDouble
  }

  val slots: Double = numContainers.toDouble

  val mapRatio = 1.0d

  val reduceRatio = 1.0d

  val mapSlots = mapRatio * slots

  val reduceSlots = reduceRatio * slots

  lazy val avgMap = simulation avg MapTask

  lazy val avgReduce = simulation avg ReduceTask

  lazy val avgShuffle = simulation avg ShuffleTask

  lazy val maxMap = simulation max MapTask

  lazy val maxReduce = simulation max ReduceTask

  lazy val maxShuffle = simulation max ShuffleTask

  lazy val numMap = simulation numOf MapTask

  lazy val numReduce = simulation numOf ReduceTask

  lazy val upperBound = (avgMap * numMap - 2 * maxMap) / mapSlots +
    ((avgReduce + avgShuffle) * numReduce - 2 * (maxReduce + maxShuffle)) / reduceSlots +
    2 * (maxMap + maxReduce + maxShuffle)

  lazy val lowerBound = (avgMap * numMap + (avgReduce + avgShuffle) * numReduce) / slots

  lazy val avg = (upperBound + lowerBound) / 2

}
