package profiler

/**
  * @author Alessandro
  */
class PreciseBound (override val mapRatio : Double, override val reduceRatio : Double,
                    override val simulation : Simulation, override val numCores : Int)
  extends Bounds(simulation, numCores)
