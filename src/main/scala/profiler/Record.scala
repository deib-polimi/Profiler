/**
 *
 */
package profiler

import java.text.SimpleDateFormat
import java.util.Locale

/**
 * @author Alessandro
 *
 */
case class Record (name : String, durationMSec : Long, startMSec : Long,
                   stopMSec : Long, taskType : TaskType, location : String) {

  def asShuffle = Record (name, durationMSec, startMSec, stopMSec, ShuffleTask, location)

  def - (other : Record) = Record (name, durationMSec - other.durationMSec,
    startMSec, stopMSec, taskType, location)

  def changeType(newType : TaskType) = Record(name, durationMSec, startMSec, stopMSec, newType, location)

}

object Record {

  private def getType(input : String) : TaskType =
    if(input.split("_")(4).toInt == 0) MapTask
    else ReduceTask

  private def parseDate (input : String) =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH).parse(input).getTime

  def apply(text : String) : Record = {
    val fields = text.split("\t")
    if(fields.size < 2) throw new Exception("Wrong entry!")
    else if(fields.size < 5) Record(fields(0), fields(1).toInt, 0, 0, getType(fields(0)), "")
    else Record(fields(0), fields(1).toInt, parseDate(fields(2)), parseDate(fields(3)), getType(fields(0)), fields(4))
  }

}
