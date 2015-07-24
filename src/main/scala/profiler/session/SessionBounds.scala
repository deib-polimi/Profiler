package profiler.session

import mapreduce.Bounds
import mapreduce.Simulation

/**
 * @author Alessandro
 */
class SessionBounds(queue : Queue, override val simulation : Simulation, override val numCores : Int) extends Bounds(simulation, numCores){

    override def slots : Double = numCores.toDouble * queue.ratio / queue.users;

//override def mapRatio = queue.mapRatio;

//override def reduceRatio = queue.reduceRatio;

}
