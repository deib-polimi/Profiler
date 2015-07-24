/**
 *
 */
package mapreduce

import java.text.SimpleDateFormat
import java.util.Locale

/**
 * @author Alessandro
 *
 */
case class Record(val name : String, val durationMSec : Long, val startMSec : Long, val stopMSec : Long, val taskType : TaskType, val location : String) {
  
  def asShuffle = Record(name, durationMSec, startMSec, stopMSec, SHUFFLE, location);
  
  def - (other : Record) = Record(name, durationMSec - other.durationMSec, startMSec, stopMSec, taskType, location);

}

object Record {  
  
  private def getType(input : String) : TaskType = 
    if(input.split("_")(4).toInt == 0) MAP;
    else REDUCE;  
  
  private def parseData(input : String) = 
    new SimpleDateFormat("yyyy-MM-dd hh:mm:ss", Locale.ENGLISH).parse(input).getTime;
  
  def apply(text : String) : Record = {
    val fields = text.split("\t");
    if(fields.size < 2) throw new Exception("Wrong entry!");
    else if(fields.size < 5) Record(fields(0), fields(1).toInt, 0, 0, getType(fields(0)), "");
    else Record(fields(0), fields(1).toInt, parseData(fields(2)), parseData(fields(3)), getType(fields(0)), fields(4));    
  }
  
}