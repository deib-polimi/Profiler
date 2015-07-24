/**
 *
 */
package profiler

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

    def validate(s2 : Simulation, s1 : Simulation, nCores : Int) = {
	val b = Bounds(s1, nCores);//28*4);
	val res = s2.validate(b);
	//println(s1.size + " " + s2.size)
	//println("OV: " + s2.over(b));
	//println("UN: " + s2.under(b));
	//println("AVG: " + avg(res) );
	avg(res);

    }

    def main(args: Array[String]): Unit = {
	//val durations = Duration(Source.fromFile("/workspace/CCSEP/cp/Utili/backup250Results/Q3/appDuration.txt").mkString);
	//val lines = Source.fromFile("/workspace/CCSEP/cp/Utili/backup250Results/Q3/results/taskDurationLO.txt").mkString;
	//val sim = Simulation(lines, durations);
	//val sim = Simulation(Source.fromFile("/workspace/CCSEP/cp/Utili/fetched40SS/Q3/results/taskDurationLO.txt").mkString);
	///home/alessandro/CCSEp2/script-fabio/fetched/R1/
	val nCores = 30*8;
	val sim = Simulation.fromDir("/workspace/Query/R1");///workspace/Query/R3/")
	//sim.executions.foreach { x => println(x.tasks.size) }
	println(sim.size)
	val validation = sim.kFold(2).map(x => validate(x._1, x._2, nCores));

	/*val res = for(
        i <- Range(1,300)
        ) yield (i, avg(sim.kFold(10).map(x => validate(x._1, x._2, i))));*/

	//res.foreach(x => println(x));
	println("Numero core: " + nCores)
	println("Numero jobs:" + sim.executions.size)
	println("Errore medio: " + avg(validation)+"%");
	//sim.all(REDUCE).foreach(x => println(x.durationMSec))

	println("Min MAP: " + sim.min(MAP))
	println("Avg MAP: " + sim.avg(MAP))
	println("Med MAP: " + (sim.min(MAP) + sim.max(MAP))/2)
	println("Max MAP: " + sim.max(MAP))
	println("Min REDUCE: " + sim.min(REDUCE))
	println("Avg REDUCE: " + sim.avg(REDUCE))
	println("Med REDUCE: " + (sim.min(REDUCE) + sim.max(REDUCE))/2)
	println("Max REDUCE: " + sim.max(REDUCE))
	println("Min SHUFFLE: " + sim.min(SHUFFLE))
	println("Avg SHUFFLE: " + sim.avg(SHUFFLE))
	println("Med SHUFFLE: " + (sim.min(SHUFFLE) + sim.max(SHUFFLE))/2)
	println("Max SHUFFLE: " + sim.max(SHUFFLE))

	println("Numero MAP: " + sim.numOf(MAP));
	println("Numero REDUCE " + sim.numOf(REDUCE));

	println("Min esecuzione: " + sim.min)
	println("Avg esecuzione: " + sim.avg)
	println("Max esecuzione: " + sim.max)
	val bounds = Bounds(sim, nCores);

	println("Low bound: " + bounds.lowerBound)
	println("Avg bound: " + bounds.avg)
	println("Upp bound: " + bounds.upperBound)

	/*
    val max = 9d;
    println("Errore: " + avg(sim.validate(new PreciseBound(1, 1, sim, nCores))));
    val x = for{
      i <- Range(1, max.toInt+1).par;
      j <- Range(1, max.toInt+1).par
    } yield (i/max, j/max, avg(sim.validate(new PreciseBound(i/max, j/max, sim, nCores))));
    x.toList.sortBy(_._3).foreach(x => println(x))*/

	/*val s1 = sim.range(0, 20);
    val s2 = sim.range(20, 99);
    val b = Bounds(s1, 28*4);

    println("LOW: " + b.lowerBound)
    println("UPP: " + b.upperBound)
    println("Average: " + sim.avg)
    println("Computed: " + b.avg)*/

	//val res = s2.validate(b);
	//sim.executions.foreach(x => println("Duration: " + x.duration));
	//res.foreach(x => println(x))
	//println("OV: " + s2.over(b));
	//println("UN: " + s2.under(b));
	//res.foreach(x => println(x))
	//println("AVG: " + avg(res) );

	//println(s1.max(MAP))
	//println(s1.max(REDUCE))
	//println(s1.avg(MAP))
	//println(s1.avg(REDUCE))
    }

}
