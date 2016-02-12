package profiler

import java.io.File

/**
  * @author Alessandro
  *
  */
class Loader(simulations: Map[String, Simulation]) {

  def performProfiling(nCores: Int): Unit = {
    simulations foreach { case (id, simulation) =>
      println(s"Application class: $id")
      if (simulation.isNonTrivialDag) dagOutput(simulation, nCores)
      else mapReduceOutput(simulation, nCores)
      println()
    }
  }

  private def mapReduceOutput(simulation: Simulation, nCores: Int): Unit = {
    println(s"Number of cores: $nCores")
    println(s"Number of jobs: ${simulation.size}")

    println(s"Min MAP: ${simulation min MapTask} ms")
    println(s"Avg MAP: ${simulation avg MapTask} ms")
    println(s"Max MAP: ${simulation max MapTask} ms")

    println(s"Min REDUCE: ${simulation min ReduceTask} ms")
    println(s"Avg REDUCE: ${simulation avg ReduceTask} ms")
    println(s"Max REDUCE: ${simulation max ReduceTask} ms")

    println(s"Min SHUFFLE: ${simulation min ShuffleTask} ms")
    println(s"Avg SHUFFLE: ${simulation avg ShuffleTask} ms")
    println(s"Max SHUFFLE: ${simulation max ShuffleTask} ms")

    println(s"Min shuffle bytes: ${simulation.minShuffleBytes} B")
    println(s"Avg shuffle bytes: ${simulation.avgShuffleBytes} B")
    println(s"Max shuffle bytes: ${simulation.maxShuffleBytes} B")

    println(s"MAP tasks: ${simulation numOf MapTask}")
    println(s"REDUCE tasks: ${simulation numOf ReduceTask}")

    println(s"Min completion time: ${simulation.min} ms")
    println(s"Avg completion time: ${simulation.avg} ms")
    println(s"Max completion time: ${simulation.max} ms")

    val bounds = Bounds(simulation, nCores)
    println(s"Low bound: ${bounds.lowerBound} ms")
    println(s"Avg bound: ${bounds.avg} ms")
    println(s"Upp bound: ${bounds.upperBound} ms")
  }

  private def dagOutput(simulation: Simulation, nCores: Int): Unit = {
    println(s"Number of cores: $nCores")
    println(s"Number of jobs: ${simulation.size}")

    simulation.vertices foreach {
      vertex =>
        println(s"Min $vertex: ${simulation min vertex} ms")
        println(s"Avg $vertex: ${simulation avg vertex} ms")
        println(s"Max $vertex: ${simulation max vertex} ms")
        if (vertex contains "Shuffle") {
          println(s"Min bytes $vertex: ${simulation minShuffleBytes vertex} B")
          println(s"Avg bytes $vertex: ${simulation avgShuffleBytes vertex} B")
          println(s"Max bytes $vertex: ${simulation maxShuffleBytes vertex} B")
        }
        println(s"$vertex tasks: ${simulation numOf vertex}")
    }

    println(s"Min completion time: ${simulation.min} ms")
    println(s"Avg completion time: ${simulation.avg} ms")
    println(s"Max completion time: ${simulation.max} ms")
  }

  def listRuns(nCores: Int, dataSize: Int): Unit = {
    simulations foreach { case (id, simulation) =>
      println(s"Application class: $id")
      // The number of cores must be the last column for compatibility with hadoop-svm
      println("complTime,nM,nR,Mavg,Mmax,Ravg,Rmax,SHavg,SHmax,Bavg,Bmax,dataSize,nCores")
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
    builder append execution.numMap
    builder append ','
    builder append execution.numReduce
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
    builder append { execution.avgShuffleBytes }
    builder append ','
    builder append { execution.maxShuffleBytes }
    builder append ','
    builder append dataSize
    builder append ','
    builder append numCores
    println (builder.toString())
  }

  def listTaskDurations(): Unit = {
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

object Loader {
  def apply(inputDirectory: File): Loader = {
    val simulations = Simulation fromDir inputDirectory
    new Loader(simulations)
  }
}
