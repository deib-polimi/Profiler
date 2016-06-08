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

import java.io.File

class Loader(simulations: Map[String, Simulation]) {

  def performProfiling(nContainers: Int): Unit = {
    simulations foreach { case (id, simulation) =>
      println(s"Application class: $id")
      if (simulation.isNonTrivialDag) dagOutput(simulation, nContainers)
      else mapReduceOutput(simulation, nContainers)
      println()
    }
  }

  private def mapReduceOutput(simulation: Simulation, nContainers: Int): Unit = {
    println(s"Number of containers: $nContainers")
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

    val bounds = Bounds(simulation, nContainers)
    println(s"Low bound: ${bounds.lowerBound} ms")
    println(s"Avg bound: ${bounds.avg} ms")
    println(s"Upp bound: ${bounds.upperBound} ms")
  }

  private def dagOutput(simulation: Simulation, nContainers: Int): Unit = {
    println(s"Number of containers: $nContainers")
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

  def listRuns(nContainers: Int, dataSize: Int): Unit = {
    simulations foreach { case (id, simulation) =>
      println(s"Application class: $id")
      if (simulation.isNonTrivialDag) printSimulationDAG(simulation, nContainers, dataSize)
      else printSimulationMapReduce(simulation, nContainers, dataSize)
    }
  }

  private def printSimulationMapReduce(simulation: Simulation, numContainers: Int, dataSize: Int): Unit = {
    // The number of containers must be the last column for compatibility with hadoop-svm
    println("complTime,nM,nR,Mavg,Mmax,Ravg,Rmax,SHavg,SHmax,Bavg,Bmax,users,dataSize,nContainers")
    simulation.executions foreach {
      printDataMapReduce(_, numContainers, dataSize, simulation.users)
    }
    println()
  }

  private def printSimulationDAG(simulation: Simulation, numContainers: Int, dataSize: Int): Unit = {
    val builder = new StringBuilder
    builder append "complTime,"
    val columns = simulation.vertices flatMap {
      case vertex if vertex contains "Shuffle" =>
        val number = { vertex split " " }.last
        val stage = s"S$number"
        Seq(s"${stage}avg", s"${stage}max", s"${stage}Bavg", s"${stage}Bmax")
      case vertex =>
        val stage = vertex split " " map { token =>
          if (token forall Character.isDigit) token
          else token.head
        } mkString ""
        Seq(s"n$stage", s"${stage}avg", s"${stage}max")
    }
    builder append { columns mkString "," }
    // The number of containers must be the last column for compatibility with hadoop-svm
    builder append ",users,dataSize,nContainers"
    println(builder.toString())
    simulation.executions foreach {
      printDataDAG(_, numContainers, dataSize, simulation.users)
    }
    println()
  }

  private def printDataMapReduce(execution: Execution, numContainers: Int, dataSize: Int, users: Int): Unit = {
    val builder = new StringBuilder
    builder append execution.duration append ','
    builder append execution.numMap append ','
    builder append execution.numReduce append ','
    builder append { execution avg MapTask } append ','
    builder append { execution max MapTask } append ','
    builder append { execution avg ReduceTask } append ','
    builder append { execution max ReduceTask } append ','
    builder append { execution avg ShuffleTask } append ','
    builder append { execution max ShuffleTask } append ','
    builder append { execution.avgShuffleBytes } append ','
    builder append { execution.maxShuffleBytes } append ','
    builder append users append ','
    builder append dataSize append ','
    builder append numContainers
    println (builder.toString())
  }

  private def printDataDAG(execution: Execution, numContainers: Int, dataSize: Int, users: Int): Unit = {
    val builder = new StringBuilder
    builder append execution.duration append ','
    execution.vertices foreach {
      case vertex if vertex contains "Shuffle" =>
        builder append { execution avg vertex } append ','
        builder append { execution max vertex } append ','
        builder append { execution avgShuffleBytes vertex } append ','
        builder append { execution maxShuffleBytes vertex } append ','
      case vertex =>
        builder append { execution numTasks vertex } append ','
        builder append { execution avg vertex } append ','
        builder append { execution max vertex } append ','
    }
    builder append users append ','
    builder append dataSize append ','
    builder append numContainers
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

  def listTaskDataByRun[T](data: Record => T): Unit = {
    simulations foreach { case (id, simulation) =>
      println(s"Application class: $id")
      simulation.executions foreach { execution =>
        println(s"Task ID: ${ execution.taskId }")
        execution.vertices foreach { vertex =>
          println(s"$vertex:")
          execution tasks vertex map data foreach println
          println()
        }
      }
    }
  }

  def listTaskDataByRunAndNode[T](data: Record => T): Unit = {
    simulations foreach { case (id, simulation) =>
      println(s"Application class: $id")
      simulation.executions foreach { execution =>
        println(s"Task ID: ${ execution.taskId }")
        execution.vertices foreach { vertex =>
          println(s"$vertex:")
          execution.nodes foreach { node =>
            println(s"$node:")
            execution tasks (vertex, node) map data foreach println
            println()
          }
        }
      }
    }
  }

  def performProfilingByNode(nContainers: Int): Unit = {
    simulations foreach { case (id, simulation) =>
      println(s"Application class: $id")
      dagOutputByNode(simulation, nContainers)
      println()
    }
  }

  private def dagOutputByNode(simulation: Simulation, nContainers: Int): Unit = {
    println(s"Number of containers: $nContainers")
    println(s"Number of jobs: ${simulation.size}")
    println()

    simulation.nodes foreach {
      node =>
        println(s"Node: $node")
        simulation.vertices foreach {
          vertex =>
            println(s"Min $vertex: ${ simulation min (vertex, node) } ms")
            println(s"Avg $vertex: ${ simulation avg (vertex, node) } ms")
            println(s"Max $vertex: ${ simulation max (vertex, node) } ms")
            println(s"$vertex tasks: ${ simulation avgNumOf (vertex, node) }")
        }
        println()
    }

    println(s"Min completion time: ${simulation.min} ms")
    println(s"Avg completion time: ${simulation.avg} ms")
    println(s"Max completion time: ${simulation.max} ms")
  }

  def listTaskDurationsByNode(): Unit = {
    simulations foreach { case (id, simulation) =>
      println(s"Application class: $id")
      simulation.nodes foreach { node =>
        println(s"Node: $node")
        simulation.vertices foreach { vertex =>
          println(s"$vertex:")
          simulation all (vertex, node) foreach { task => println(task.durationMSec) }
          println()
        }
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
