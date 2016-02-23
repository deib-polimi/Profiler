package session

import profiler.{Bounds, Simulation}

/**
  * @author Alessandro
  */
class SessionBounds(queue: Queue, override val simulation: Simulation,
                    override val numCores: Int) extends Bounds (simulation, numCores) {

  override val slots: Double = numCores.toDouble * queue.ratio / queue.users
}