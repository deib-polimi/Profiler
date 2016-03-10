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
package common

import java.io.File

import profiler.Loader
import session.Session

import scala.annotation.tailrec

object Main {

  private final val USAGE =
    """usage:
      |  Profiler -p|--single-class directory cores
      |  Profiler -l|--list-runs directory cores dataset_size
      |  Profiler -t|--list-tasks directory
      |  Profiler -s|--session -c deadline -q queue1=alpha1,queue2=alpha2,queue3=alpha3,queue4=alpha4 -d profiles_directory directory cores
      |""".stripMargin

  private final val WRONG_INPUT_ARGUMENTS = "error: unrecognized input arguments"

  def main(args: Array[String]): Unit = {
    if (args.length == 0) Console.err println USAGE
    else processArguments(args.toList)
  }

  private def processArguments(args: List[String]): Unit = args.head match {
    case "-p" | "--single-class" => profiler(args.tail)
    case "-s" | "--session" => session(args.tail)
    case "-l" | "--list-runs" => runs(args.tail)
    case "-t" | "--list-tasks" => tasks(args.tail)
    case _ =>
      Console.err println WRONG_INPUT_ARGUMENTS
      Console.err println USAGE
  }

  private def profiler(args: Seq[String]): Unit =
    parseArgumentsForProfiling(args) match {
      case Some(tuple) =>
        val (inputDirectory, nCores) = tuple
        Loader(inputDirectory) performProfiling nCores
      case None =>
        Console.err println WRONG_INPUT_ARGUMENTS
        Console.err println USAGE
    }

  private def parseArgumentsForProfiling(args: Seq[String]): Option[(File, Int)] =
    try {
      val inputDirectory = new File(args.head).getAbsoluteFile
      val nCores = args(1).toInt
      Some(inputDirectory -> nCores)
    } catch {
      case e: IndexOutOfBoundsException => None
    }

  private def runs(args: Seq[String]): Unit =
    parseArgumentsForListingRuns(args) match {
      case Some((inputDirectory, nCores, dataSize)) =>
        Loader(inputDirectory) listRuns (nCores, dataSize)
      case None =>
        Console.err println WRONG_INPUT_ARGUMENTS
        Console.err println USAGE
    }

  private def parseArgumentsForListingRuns(args: Seq[String]): Option[(File, Int, Int)] =
    try {
      val inputDirectory = new File(args.head).getAbsoluteFile
      val nCores = args(1).toInt
      val dataSize = args(2).toInt
      Some((inputDirectory, nCores, dataSize))
    } catch {
      case e: IndexOutOfBoundsException => None
    }

  private def session(args: List[String]): Unit = {
    type OptionMap = Map[Symbol, String]

    @tailrec
    def nextArgument(map: OptionMap, args: List[String]): Option[OptionMap] = args match {
      case Nil => Some(map)
      case "-c" :: deadline :: tail => nextArgument(map + ('deadline -> deadline), tail)
      case "-d" :: directory :: tail => nextArgument(map + ('profiles -> directory), tail)
      case "-q" :: queues :: tail => nextArgument(map + ('queues -> queues), tail)
      case directory :: cores :: tail => nextArgument(map +('input -> directory, 'cores -> cores), tail)
      case _ => None
    }

    nextArgument(Map(), args) match {
      case Some(options) =>
        val inputDir = new File(options('input)).getAbsoluteFile
        val profilesDir = new File(options('profiles)).getAbsoluteFile
        val nCores = options('cores).toInt
        val deadline = options('deadline).toInt
        val queues = for (entry <- options('queues) split ",") yield {
          val pieces = entry split "="
          pieces(0) -> pieces(1).toDouble
        }
        Session mainEntryPoint (inputDir, profilesDir, nCores, deadline, queues: _*)
      case None =>
        Console.err println WRONG_INPUT_ARGUMENTS
        Console.err println USAGE
    }
  }

  private def tasks(args: List[String]): Unit = args match {
    case dir :: Nil =>
      val directory = new File(dir)
      Loader(directory) listTaskDurations ()
    case _ =>
      Console.err println WRONG_INPUT_ARGUMENTS
      Console.err println USAGE
  }
}
