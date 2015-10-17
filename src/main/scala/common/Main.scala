package common

import java.io.File

import profiler.Loader
import session.Session

/**
 * @author eugenio
 */
object Main {

  private final val USAGE =
    """usage:
      |  Profiler -p|--single-class directory cores
      |  Profiler -l|--list-runs directory cores
      |  Profiler -s|--session -c deadline -q queue1=alpha1,queue2=alpha2,queue3=alpha3,queue4=alpha4 -d profiles_directory directory cores
      |""".stripMargin

  private final val WRONG_INPUT_ARGUMENTS = "error: unrecognized input arguments"

  def main (args : Array [String]) : Unit = {
    if (args.length == 0) Console.err println USAGE
    else processArguments (args.toList)
  }

  private def processArguments (args : List [String]) : Unit = args.head match {
    case "-p" | "--single-class" => profiler (args.tail.toIterator)
    case "-s" | "--session" => session (args.tail)
    case "-l" | "--list-runs" => list (args.tail.toIterator)
    case _ =>
      Console.err println WRONG_INPUT_ARGUMENTS
      Console.err println USAGE
  }

  private def profiler (args: Iterator [String]): Unit =
    parseArgumentsForLoader(args) match {
      case Some(tuple) =>
        val nCores = tuple._2
        val inputDirectory = tuple._1
        Loader.performProfiling (nCores, inputDirectory)
      case None =>
        Console.err println WRONG_INPUT_ARGUMENTS
        Console.err println USAGE
    }

  private def list (args: Iterator [String]): Unit =
    parseArgumentsForLoader(args) match {
      case Some(tuple) =>
        val nCores = tuple._2
        val inputDirectory = tuple._1
        Loader.listRuns (nCores, inputDirectory)
      case None =>
        Console.err println WRONG_INPUT_ARGUMENTS
        Console.err println USAGE
    }

  private def parseArgumentsForLoader (args: Iterator [String]): Option[(File, Int)] =
    try {
      val inputDirectory = new File (args.next()).getAbsoluteFile
      val nCores = args.next().toInt
      Some(inputDirectory -> nCores)
    } catch {
      case nsee : NoSuchElementException => None
    }

  private def session (args: List [String]): Unit = {
    type OptionMap = Map [Symbol, String]
    def nextArgument (map : OptionMap, args : List [String]) : OptionMap = args match {
      case Nil => map
      case "-c" :: deadline :: tail => nextArgument (map + ('deadline -> deadline), tail)
      case "-d" :: directory :: tail => nextArgument (map + ('profiles -> directory), tail)
      case "-q" :: queues :: tail => nextArgument (map + ('queues -> queues), tail)
      case directory :: cores :: Nil => map + ('input -> directory, 'cores -> cores)
      case _ => throw new RuntimeException
    }

    try {
      val options = nextArgument (Map (), args)
      val inputDir = new File (options ('input)).getAbsoluteFile
      val profilesDir = new File (options ('profiles)).getAbsoluteFile
      val nCores = options ('cores).toInt
      val deadline = options ('deadline).toInt
      val queues = for (entry <- options ('queues) split ",") yield {
        val pieces = entry split "="
        pieces(0) -> pieces (1).toDouble
      }
      Session.mainEntryPoint (inputDir, profilesDir, nCores, deadline, queues:_*)
    } catch {
      case re : RuntimeException =>
        Console.err println WRONG_INPUT_ARGUMENTS
        Console.err println USAGE
    }
  }
}
