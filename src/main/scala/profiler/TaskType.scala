package profiler

/**
  * @author Alessandro
  *
  */
sealed abstract class TaskType

case object MapTask extends TaskType

case object ReduceTask extends TaskType

case object ShuffleTask extends TaskType
