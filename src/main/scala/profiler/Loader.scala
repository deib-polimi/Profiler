package profiler

import java.io.File

/**
  * @author Alessandro
  *
  */
object Loader {

  private def avg(input: Seq[Double]): Double = input.sum / input.size

  private def validate(s2: Simulation, s1: Simulation, nCores: Int): Double = {
    val b = Bounds(s1, nCores)
    val res = s2 validate b
    avg(res)
  }

  def performProfiling(nCores: Int, inputDirectory: File): Unit = {
    val simulations = Simulation fromDir inputDirectory
    simulations foreach { case (id, simulation) =>
      println(s"Application class: $id")
      if (simulation.isNonTrivialDag) dagOutput(simulation, nCores)
      else mapReduceOutput(simulation, nCores)
      println()
    }
  }

  private def mapReduceOutput(simulation: Simulation, nCores: Int): Unit = {
    val validation = simulation kFold 2 map { x => validate(x._1, x._2, nCores) }

    println(s"Number of cores: $nCores")
    println(s"Number of jobs: ${simulation.size}")
    println(s"Average error: ${avg(validation)} %")

    println(s"Min MAP: ${simulation min MapTask}")
    println(s"Avg MAP: ${simulation avg MapTask}")
    println(s"Max MAP: ${simulation max MapTask}")

    println(s"Min REDUCE: ${simulation min ReduceTask}")
    println(s"Avg REDUCE: ${simulation avg ReduceTask}")
    println(s"Max REDUCE: ${simulation max ReduceTask}")

    println(s"Min SHUFFLE: ${simulation min ShuffleTask}")
    println(s"Avg SHUFFLE: ${simulation avg ShuffleTask}")
    println(s"Max SHUFFLE: ${simulation max ShuffleTask}")

    println(s"MAP tasks: ${simulation numOf MapTask}")
    println(s"REDUCE tasks: ${simulation numOf ReduceTask}")

    println(s"Min completion time: ${simulation.min}")
    println(s"Avg completion time: ${simulation.avg}")
    println(s"Max completion time: ${simulation.max}")

    val bounds = Bounds(simulation, nCores)
    println(s"Low bound: ${bounds.lowerBound}")
    println(s"Avg bound: ${bounds.avg}")
    println(s"Upp bound: ${bounds.upperBound}")
  }

  private def dagOutput(simulation: Simulation, nCores: Int): Unit = {
    println(s"Number of cores: $nCores")
    println(s"Number of jobs: ${simulation.size}")

    simulation.vertices foreach {
      vertex =>
        println(s"Min $vertex: ${simulation min vertex}")
        println(s"Avg $vertex: ${simulation avg vertex}")
        println(s"Max $vertex: ${simulation max vertex}")
        println(s"$vertex tasks: ${simulation numOf vertex}")
    }

    println(s"Min completion time: ${simulation.min}")
    println(s"Avg completion time: ${simulation.avg}")
    println(s"Max completion time: ${simulation.max}")
  }

  def listRuns(nCores: Int, inputDirectory: File, dataSize: Int): Unit = {
    val simulations = Simulation fromDir inputDirectory
    simulations foreach { case (id, simulation) =>
      println(s"Application class: $id")
      // The number of cores must be the last column for compatibility with hadoop-svm
      println("complTime,nM,nR,Mavg,Mmax,Ravg,Rmax,SHavg,SHmax,dataSize,nCores")
      simulation.executions foreach {
        printData(_, nCores, dataSize)
      }
      println()
    }
  }

  private def printData(execution: Execution, numCores: Int, dataSize: Int): Unit = {
    val builder = new StringBuilder
    builder append execution.duration
    builder append ','
    builder append { execution numTasks MapTask }
    builder append ','
    builder append { execution numTasks ReduceTask }
    builder append ','
    builder append { execution avg MapTask }
    builder append ','
    builder append { execution max MapTask }
    builder append ','
    builder append { execution avg ReduceTask }
    builder append ','
    builder append { execution max ReduceTask }
    builder append ','
    builder append { execution avg ShuffleTask }
    builder append ','
    builder append { execution max ShuffleTask }
    builder append ','
    builder append dataSize
    builder append ','
    builder append numCores
    println (builder.toString())
  }

  def listTaskDurations(inputDirectory: File): Unit = {
    val simulations = Simulation fromDir inputDirectory
    simulations foreach { case (id, simulation) =>
      println(s"Application class: $id")
      simulation.vertices foreach { vertex =>
        println(s"$vertex:")
        simulation all vertex foreach { task => println(task.durationMSec) }
        println()
      }
    }
  }
}
