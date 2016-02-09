package profiler

class ExternalExecution(override val name: String, external: Duration,
                             override val tasks: Seq[Record])
  extends Execution(name, tasks) {

  override lazy val duration = {
    val names = tasks map { _.name }
    external obtainTotalDuration names
  }

}
