/**
 *
 */
package profiler

import java.io.File

/**
 * @author Alessandro
 *
 */
object Loader {

    def min(input : Seq[Int]) : Int = input.reduce((x,y) => Math.min(x,y));

    def max(input : Seq[Int]) : Int = input.reduce((x,y) => Math.max(x,y));

    def avg(input : Seq[Double]) : Double = {
	    input.reduce((x,y) => x+y)/input.size;
    }

    def validate(s2 : Simulation, s1 : Simulation, nCores : Int): Double = {
	    val b = Bounds(s1, nCores)
	    val res = s2.validate(b)
	    avg(res);
    }

    def main(args: Array[String]): Unit = {
	    var nCores = 30 * 8
      var inputDirectory = new File ("/workspace/Query/R1")
      if (args.length >= 1) inputDirectory = new File (args(0))
      if (args.length >= 2) nCores = Integer parseInt args(1)

	    val sim = Simulation.fromDir(inputDirectory)

	    println(sim.size)
	    val validation = sim.kFold(2).map(x => validate(x._1, x._2, nCores))

	    println("Numero core: " + nCores)
	    println("Numero jobs:" + sim.executions.size)
	    println("Errore medio: " + avg(validation)+"%")

	    println("Min MAP: " + sim.min(MAP))
	    println("Avg MAP: " + sim.avg(MAP))
	    println("Max MAP: " + sim.max(MAP))
	    println("Min REDUCE: " + sim.min(REDUCE))
	    println("Avg REDUCE: " + sim.avg(REDUCE))
	    println("Max REDUCE: " + sim.max(REDUCE))
	    println("Min SHUFFLE: " + sim.min(SHUFFLE))
	    println("Avg SHUFFLE: " + sim.avg(SHUFFLE))
	    println("Max SHUFFLE: " + sim.max(SHUFFLE))

	    println("MAP tasks: " + sim.numOf(MAP))
	    println("REDUCE tasks: " + sim.numOf(REDUCE))

	    println("Min completion time: " + sim.min)
	    println("Avg completion time: " + sim.avg)
	    println("Max completion time: " + sim.max)

	    val bounds = Bounds(sim, nCores);
	    println("Low bound: " + bounds.lowerBound)
	    println("Avg bound: " + bounds.avg)
	    println("Upp bound: " + bounds.upperBound)
    }
}
