/**
 *
 */
package profiler

/**
 * @author Alessandro
 *
 */
abstract case class Execution(name : String, tasks : Array[Record]) {

    def numTasks(taskType : TaskType) : Int = tasks.count(_.taskType == taskType);

    def numMap : Int = numTasks(MAP);

    def numReduce : Int = numTasks(REDUCE);

    //private def start : Long = tasks.map(_.startMSec).reduce( (x,y) => Math.min(x, y));

    //private def stop : Long = tasks.map(_.stopMSec).reduce( (x,y) => Math.max(x, y));

    def duration : Long;// = stop - start;

    def locations : Set[String] = tasks.map(_.location).toSet;

    def tasks(taskType : TaskType) : Array[Record] = tasks.filter(_.taskType == taskType);

    def sum(taskType : TaskType) : Long = tasks(taskType).map(_.durationMSec).reduce( (x,y) => x + y);

    def max(taskType : TaskType) : Long = tasks(taskType).map(_.durationMSec).reduce( (x,y) => Math.max(x,y) );

    def min(taskType : TaskType) : Long = tasks(taskType).map(_.durationMSec).reduce( (x,y) => Math.min(x,y) );

}

object Execution {

    /*def apply(text : String) : Execution = {
    val lines = text.split("\n");
    Execution(lines.map(Record(_)));
  }*/

    def apply(text : String, duration : Duration, shuffle : Shuffle) : Execution = {
	    val lines = text.split("\n");
	    new ExternalExecution(lines.head, duration, shuffle(lines.map(Record(_))));
    }

}
