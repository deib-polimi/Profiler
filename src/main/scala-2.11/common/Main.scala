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

import profiler.{Loader, Record}
import session.Session

import scala.annotation.tailrec

object Main {

  private final val USAGE =
    """usage:
      |  Profiler -p|--single-class [--by-node] directory containers
      |  Profiler -l|--list-runs directory containers dataset_size
      |  Profiler -t|--list-tasks [--by-node] directory
      |  Profiler -d|--list-data [--by-node] -a|-c|-d|-n directory
      |  Profiler -s|--session -c deadline -q queue1=alpha1,queue2=alpha2,queue3=alpha3,queue4=alpha4 -d profiles_directory directory containers
    """.stripMargin

  private final val HELP =
    """optional files:
      |  appId.txt, example line: R1  application_1463474628860_0035
      |  appUsers.txt, example line:  R2  4
      |  dependencies.json, example:  {"R4": {"Reducer 2": ["Map 1"]}}
    """.stripMargin

  private final val WRONG_INPUT_ARGUMENTS = "error: unrecognized input arguments"

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      Console.err println USAGE
      System exit 1
    }
    else processArguments(args.toList)
  }

  private def processArguments(args: List[String]): Unit = args.head match {
    case "-p" | "--single-class" => profiler(args.tail)
    case "-s" | "--session" => session(args.tail)
    case "-l" | "--list-runs" => runs(args.tail)
    case "-t" | "--list-tasks" => tasks(args.tail)
    case "-d" | "--list-data" => data(args.tail)
    case "-h" | "--help" => help()
    case _ => error()
  }

  private def profiler(args: Seq[String]): Unit = {
    val (byNode, nextArgs) = processByNode(args)
    parseArgumentsForProfiling(nextArgs) match {
      case Some((inputDirectory, nContainers)) =>
        val loader = Loader(inputDirectory)
        if (byNode) loader performProfilingByNode nContainers
        else loader performProfiling nContainers
      case None => error()
    }
  }

  private def parseArgumentsForProfiling(args: Seq[String]): Option[(File, Int)] =
    try {
      val inputDirectory = new File(args.head).getAbsoluteFile
      val nContainers = args(1).toInt
      Some(inputDirectory -> nContainers)
    } catch {
      case e: IndexOutOfBoundsException => None
    }

  private def runs(args: Seq[String]): Unit =
    parseArgumentsForListingRuns(args) match {
      case Some((inputDirectory, nContainers, dataSize)) =>
        Loader(inputDirectory) listRuns (nContainers, dataSize)
      case None => error()
    }

  private def parseArgumentsForListingRuns(args: Seq[String]): Option[(File, Int, Int)] =
    try {
      val inputDirectory = new File(args.head).getAbsoluteFile
      val nContainers = args(1).toInt
      val dataSize = args(2).toInt
      Some((inputDirectory, nContainers, dataSize))
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
      case directory :: containers :: tail => nextArgument(map +('input -> directory, 'containers -> containers), tail)
      case _ => None
    }

    nextArgument(Map(), args) match {
      case Some(options) =>
        val inputDir = new File(options('input)).getAbsoluteFile
        val profilesDir = new File(options('profiles)).getAbsoluteFile
        val nContainers = options('containers).toInt
        val deadline = options('deadline).toInt
        val queues = for (entry <- options('queues) split ",") yield {
          val pieces = entry split "="
          pieces(0) -> pieces(1).toDouble
        }
        Session mainEntryPoint (inputDir, profilesDir, nContainers, deadline, queues: _*)
      case None => error()
    }
  }

  private def tasks(args: List[String]): Unit = {
    val (byNode, nextArgs) = processByNode(args)
    nextArgs match {
      case dir :: Nil =>
        val directory = new File(dir)
        val loader = Loader(directory)
        if (byNode) loader listTaskDurationsByNode ()
        else loader listTaskDurations ()
      case _ => error()
    }
  }

  private def data(args: List[String]): Unit = {
    val (byNode, nextArgs) = processByNode(args)
    val method = nextArgs.head match {
      case "-a" => Some( (r: Record) => r.startMSec )
      case "-c" => Some( (r: Record) => r.stopMSec )
      case "-d" => Some( (r: Record) => r.durationMSec )
      case "-n" => Some( (r: Record) => r.node )
      case _ => None
    }
    method match {
      case None => error()
      case Some(data) =>
        nextArgs.tail match {
          case dir :: Nil =>
            val directory = new File(dir)
            if (byNode) Loader(directory) listTaskDataByRunAndNode data
            else Loader(directory) listTaskDataByRun data
          case _ => error()
        }
    }
  }

  private def error(): Unit = {
    Console.err println WRONG_INPUT_ARGUMENTS
    Console.err println USAGE
    System exit 1
  }

  private def help(): Unit = {
    println(USAGE)
    println(HELP)
  }

  private def processByNode(args: Seq[String]): (Boolean, Seq[String]) = args.head match {
    case "--by-node" => (true, args.tail)
    case _ => (false, args)
  }
}
