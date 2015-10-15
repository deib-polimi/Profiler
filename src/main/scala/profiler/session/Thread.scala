package profiler.session

import java.io.File

import profiler.Simulation

/**
 * @author Alessandro
 */
case class Thread(user : String, query : String, queue : String, id : String, executions : Seq[Run], profile : Simulation) {

  executions.foreach(x => println("RUN: " + query + " " + executions));

  def sequence = executions;

  def avg : Long = sequence.map(_.duration).reduce((x,y)=> x+y)/(sequence.size);

  def fullId = user + " " + query + " " + id;

  def validate(queueManager : QueueManager, numCores : Int): Double = {
    val bounds = new SessionBounds(queueManager.queue(queue), profile, numCores);
    bounds.error(avg);
  };

  def validateUpper(queueManager : QueueManager, numCores : Int): Double = {
    val bounds = new SessionBounds(queueManager.queue(queue), profile, numCores);
    bounds.errorUpper(avg);
  };

  def validateWith (deadlineSeconds : Long) = (deadlineSeconds.toDouble * 1000 - avg) / avg;

}

object Thread {

  private def processFilename (filename : String): List[String] = {
    val name = filename.dropRight(4);
    name.split("_").toList;
  }

  def apply (text : String, filename : String, profileDir : File) : Thread = {
    val fields = text.split("\n").toSeq;
    processFilename (filename) match {
      case List (user, query, queue, id) => {
        val profile = Simulation.fromDir (new File (profileDir, query));
        Thread(user, query, queue, id, fields.map(Run(_)), profile);
      }
    }
  }

};
