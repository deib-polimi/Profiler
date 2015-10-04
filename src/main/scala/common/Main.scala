package common

import java.io.File
import profiler.session.QueueManager
import profiler.Loader
import profiler.session.Session

/**
 * @author eugenio
 */
object Main {
    private val usage = """
	    usage:
	    Profiler -p|--single-class directory cores
	    Profiler -s|--session -c deadline -q queue1=alpha1,queue2=alpha2,queue3=alpha3,queue4=alpha4 -d profiles_directory directory cores
	    """;

    def main (args : Array [String]) : Unit = {
	    if (args.length == 0) println (usage)
	    else processArguments (args.toList)
    }

    private def processArguments (args : List [String]) : Unit = args.head match {
    case "-p" | "--single-class" => loader (args.tail.toIterator)
    case "-s" | "--session" => session (args.tail)
    }

    private def loader (args: Iterator [String]): Unit = {
      val inputDirectory = new File (args.next)
	    val nCores = args.next.toInt
	    Loader.mainEntryPoint (nCores, inputDirectory)
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
        val inputDir = options ('input)
        val profilesDir = options ('profiles)
        val nCores = options ('cores).toInt
        val deadline = options ('deadline).toInt
        val queues = for (entry <- options ('queues) split ",") yield {
          val pieces = entry split "="
          (pieces(0) -> pieces (1).toDouble)
        }
        Session.mainEntryPoint (inputDir, profilesDir, nCores, deadline, queues:_*)
      } catch {
        case nsee : NoSuchElementException => println ("error: not enough input arguments")
        case nfe : NumberFormatException => println ("error: deadline, cores, and alpha should be numbers")
        case re : RuntimeException => {
          println ("error: malformed input arguments")
          println (usage)
        }
      }
    }
}
