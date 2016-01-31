package session

import java.io.File

import profiler._

import scala.annotation.tailrec
import scala.io.Source

class Profile (numberMap : Map[TaskType, Int], avgMap : Map[TaskType, Long], maxMap : Map[TaskType, Long])
  extends Simulation(Seq()) {

  override def avg(taskType : TaskType) : Long = avgMap(taskType)

  override def max(taskType : TaskType) : Long = maxMap(taskType)

  override def numOf(taskType : TaskType): Int = numberMap(taskType)

}

object Profile {

  def apply (directory : File) = {
    val dataDir = new File (directory, "data")
    val profilerOutput = new File (dataDir, "profile.txt")
    val (numberMap, avgMap, maxMap) = processFile (profilerOutput)
    new Profile (numberMap, avgMap, maxMap)
  }

  private def processFile (input : File) = {
    @tailrec
    def parseLine (numberMap : Map[TaskType, Int], avgMap : Map[TaskType, Long],
                   maxMap : Map[TaskType, Long], list : List[String]) :
    (Map[TaskType, Int], Map[TaskType, Long], Map[TaskType, Long]) =
      list match {
        case head :: tail =>
          head match {
            case ProfilerOutputRegex.avgMap(time) =>
              parseLine(numberMap, avgMap + (MapTask -> time.toLong), maxMap, tail)
            case ProfilerOutputRegex.maxMap(time) =>
              parseLine(numberMap, avgMap, maxMap + (MapTask -> time.toLong), tail)
            case ProfilerOutputRegex.avgReduce(time) =>
              parseLine(numberMap, avgMap + (ReduceTask -> time.toLong), maxMap, tail)
            case ProfilerOutputRegex.maxReduce(time) =>
              parseLine(numberMap, avgMap, maxMap + (ReduceTask -> time.toLong), tail)
            case ProfilerOutputRegex.avgShuffle(time) =>
              parseLine(numberMap, avgMap + (ShuffleTask -> time.toLong), maxMap, tail)
            case ProfilerOutputRegex.maxShuffle(time) =>
              parseLine(numberMap, avgMap, maxMap + (ShuffleTask -> time.toLong), tail)
            case ProfilerOutputRegex.numMap(number) =>
              parseLine(numberMap + (MapTask -> number.toInt), avgMap, maxMap, tail)
            case ProfilerOutputRegex.numReduce(number) =>
              parseLine(numberMap + (ReduceTask -> number.toInt), avgMap, maxMap, tail)
            case _ => parseLine(numberMap, avgMap, maxMap, tail)
          }
        case Nil => (numberMap, avgMap, maxMap)
      }

    val content = Source fromFile input
    parseLine(Map(), Map(), Map(), content.getLines().toList)
  }

}
