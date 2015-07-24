/**
 *
 */
package profiler

import scala.Range
import scala.io.Source

/**
 * @author Alessandro
 *
 */
case class Simulation(executions : Array[Execution]) {

    private def sum(l : Array[Long]) : Long = l.reduce( (x,y) => x + y);

    def avg : Long = executions.map(_.duration).reduce( (x,y) => x + y) / executions.length;

    def avg(taskType : TaskType) : Long = sum(executions.map(_.sum(taskType))) / sum(executions.map(_.numTasks(taskType).toLong));

    def max(taskType : TaskType) : Long = executions.map(_.max(taskType)).reduce( (x,y) => Math.max(x,y));

    def min(taskType : TaskType) : Long = executions.map(_.min(taskType)).reduce( (x,y) => Math.min(x,y));

    def max : Long = executions.map(_.duration).reduce((x,y) => Math.max(x,y));

    def min : Long = executions.map(_.duration).reduce((x,y) => Math.min(x,y));

    def all(taskType : TaskType) = executions.flatMap(_.tasks(taskType)).sortBy(_.durationMSec);

    def kFold(subdivision : Int) = {
	val size = executions.size/subdivision;
	val range = Range(0, executions.length, size);
	range.map(x => setRange(x, size)).filterNot(x => x._1.executions.size < size);
    }

    private def filter(selected : Seq[Int]) = {
	val slice = for(i <- selected) yield executions(i);
		Simulation(slice.toArray);
    }

    private def setRange(start : Int, size : Int) : (Simulation, Simulation) = {
	val all = Range(0, executions.size);
	val selected = all.filter(Range(start, start+size).contains(_));
	val unselected = all.filterNot(Range(start, start+size).contains(_));
	(filter(selected), filter(unselected));
    }

    def range(min : Int, max : Int) : Simulation = {
	val slice = for(i <- Range(min, max) ) yield executions(i);
		Simulation(slice.toArray);
    }

    def numOf(taskType : TaskType) = {
	val num = executions.map(_.numTasks(taskType)).toSet;
	//if(num.size != 1) throw new Exception("Wrong argument");
	//else num.head;
	num.reduce(Math.max(_, _));
	//num.reduce(_ + _)/num.size;
    }

    def validate(bounds : Bounds) =
	    executions.map(x => bounds.error(x.duration)*100).sortBy(x => x);

    def validateMore(bounds : Bounds) =
	    executions.map(x => (bounds.error(x.duration)*100, x.locations.size) ).sortBy(x => x._1);

    def over(bounds : Bounds) : Int = executions.count(_.duration > bounds.upperBound);

    def under(bounds : Bounds) : Int = executions.count(_.duration < bounds.lowerBound);

    def size : Int = executions.size;

}

object Simulation {

    def fromDir(dir : String) : Simulation = {
	    val durations = Duration(Source.fromFile(dir + "/data/appDuration.txt").mkString);
	    val lines = Source.fromFile(dir + "/data/taskDurationLO.txt").mkString;
	    val shuffle = Shuffle(Source.fromFile(dir + "/data/shuffleDurationLO.txt").mkString);
	    Simulation(lines, durations, shuffle);
    }

    /*def apply(text : String) : Simulation = {
    val executions = text.split("\n\n");
    Simulation(executions.map(Execution(_)));
  }*/

    def apply(text : String, duration : Duration, shuffle : Shuffle) : Simulation = {
	    val executions = text.split("\n\n").map(Execution(_, duration, shuffle)).
		    filter(x => duration.contains(x.tasks.head.name));
	    val numTask = executions.map(_.tasks.size).reduce(Math.max(_,_));
	    println(executions.size + " " + duration.size)
	    executions.foreach(x => println(x.tasks(MAP).size + " " + x.tasks(REDUCE).size));
	    //executions.filterNot {_.tasks.size == numTask}.foreach(x => println("A: " + x.name + " " + x.tasks.size));
	    Simulation(executions)//.filter(_.tasks.size == numTask));
    }
}
