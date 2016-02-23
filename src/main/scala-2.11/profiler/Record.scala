package profiler

import java.text.SimpleDateFormat
import java.util.Locale

/**
  * @author Alessandro
  *
  */
case class Record(name: String, durationMSec: Long, startMSec: Long = 0,
                  stopMSec: Long = 0, taskType: TaskType,
                  location: String = "", vertex: String = "",
                  bytes: Long = 0) {

  def -(other: Record) = {
    val nextStart = if (other.stopMSec < stopMSec) other.stopMSec else startMSec
    val nextStop = if (startMSec < other.startMSec) other.startMSec else stopMSec
    val nextDuration = durationMSec - other.durationMSec
    copy(durationMSec = nextDuration, startMSec = nextStart, stopMSec = nextStop)
  }

}

object Record {

  private def getType(input: String): TaskType =
    if (input.split("_")(4).toInt == 0) MapTask
    else ReduceTask

  private def parseDate(input: String) =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH).parse(input).getTime

  def apply(text: String): Record = {
    val fields = text split "\t"
    if (fields.size < 2) throw new RuntimeException("Wrong entry!")
    else if (fields.size < 5) Record(name = fields(0), durationMSec = fields(1).toLong,
      taskType = getType(fields(0)))
    else Record(fields(0), fields(1).toInt, parseDate(fields(2)), parseDate(fields(3)),
      getType(fields(0)), fields(4))
  }

}
