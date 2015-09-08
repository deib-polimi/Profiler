package profiler.session

import profiler.Bounds
import profiler.Simulation

/**
 * @author Alessandro
 */
class SessionBounds(queue : Queue, override val simulation : Simulation,
		override val numCores : Int) extends Bounds (simulation, numCores) {

	override def slots : Double = numCores.toDouble * queue.ratio / queue.users;
}
