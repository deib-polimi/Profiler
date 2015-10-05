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
	};

	def validate(s2 : Simulation, s1 : Simulation, nCores : Int): Double = {
			val b = Bounds(s1, nCores);
			val res = s2.validate(b);
			avg(res);
	};

  def mainEntryPoint (nCores : Int, inputDirectory : File) : Unit = {
      val sim = Simulation.fromDir(inputDirectory);

      val validation = sim.kFold(2).map(x => validate(x._1, x._2, nCores));

      println("Number of cores: " + nCores);
      println("Number of jobs: " + sim.executions.size);
      println("Average error: " + avg(validation)+"%");

      println("Min MAP: " + sim.min(MapTask));
      println("Avg MAP: " + sim.avg(MapTask));
      println("Max MAP: " + sim.max(MapTask));
      println("Min REDUCE: " + sim.min(ReduceTask));
      println("Avg REDUCE: " + sim.avg(ReduceTask));
      println("Max REDUCE: " + sim.max(ReduceTask));
      println("Min SHUFFLE: " + sim.min(ShuffleTask));
      println("Avg SHUFFLE: " + sim.avg(ShuffleTask));
      println("Max SHUFFLE: " + sim.max(ShuffleTask));

      println("MAP tasks: " + sim.numOf(MapTask));
      println("REDUCE tasks: " + sim.numOf(ReduceTask));

      println("Min completion time: " + sim.min);
      println("Avg completion time: " + sim.avg);
      println("Max completion time: " + sim.max);

      val bounds = Bounds(sim, nCores);
      println("Low bound: " + bounds.lowerBound);
      println("Avg bound: " + bounds.avg);
      println("Upp bound: " + bounds.upperBound);
  }
};
