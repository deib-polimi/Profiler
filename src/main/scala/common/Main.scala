package common

import java.io.File

import profiler.Loader
import profiler.session.Session

/**
 * @author eugenio
 */
object Main {
	private final val USAGE = """
			usage:
			Profiler -p|--single-class directory cores
			Profiler -s|--session -c deadline -q queue1=alpha1,queue2=alpha2,queue3=alpha3,queue4=alpha4 -d profiles_directory directory cores
			""";
	private final val WRONG_INPUT_ARGUMENTS = "error: unrecognized input arguments"
	private final val NUMBER_FORMAT = "error: invalid number format"
	private final val MISSING_ARGUMENTS = "error: missing input arguments"

	def main (args : Array [String]) : Unit = {
		if (args.length == 0) Console.err.println (USAGE)
		else processArguments (args.toList)
	}

	private def processArguments (args : List [String]) : Unit = args.head match {
	  case "-p" | "--single-class" => loader (args.tail.toIterator)
	  case "-s" | "--session" => session (args.tail)
	  case _ => {
		  Console.err.println (WRONG_INPUT_ARGUMENTS);
		  Console.err.println (USAGE);
	  }
	}

	private def loader (args: Iterator [String]): Unit = {
			try {
				val inputDirectory = new File (args.next).getAbsoluteFile
				val nCores = args.next.toInt
				Loader.mainEntryPoint (nCores, inputDirectory)
			} catch {
			  case nfe: NumberFormatException => Console.err.println (NUMBER_FORMAT)
			}
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
					(pieces(0) -> pieces (1).toDouble)
				}
				Session.mainEntryPoint (inputDir, profilesDir, nCores, deadline, queues:_*)
			} catch {
			  case nsee : NoSuchElementException => Console.err.println (MISSING_ARGUMENTS)
			  case nfe : NumberFormatException => Console.err.println (NUMBER_FORMAT)
			  case re : RuntimeException => {
				  Console.err.println (WRONG_INPUT_ARGUMENTS)
				  Console.err.println (USAGE)
			  }
			}
	}
}
