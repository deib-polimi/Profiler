package profiler

class ExternalExecution(override val name : String, val external : Duration,
                        tasks : Array[Record]) extends Execution(name, tasks) {

  override def duration = external.get(tasks.head.name)
}
