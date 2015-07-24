package mapreduce.session

import java.io.File
import scala.io.Source

/**
 * @author Alessandro
 */
case class Session(threads : List[Thread]) {
  
  def avg : List[(String, Long)] = threads.map(x => (x.fullId, x.avg));
  
  def validate(queueManager : QueueManager, numCores : Int) = 
    threads.map(x => x.query -> x.validate(queueManager, numCores)).groupBy(_._1).map(x => x._1 -> x._2.map(_._2).reduce(_ + _)/x._2.size)
    
  def validateUpper(queueManager : QueueManager, numCores : Int) = 
    threads.map(x => x.query -> x.validateUpper(queueManager, numCores)).groupBy(_._1).map(x => x._1 -> x._2.map(_._2).reduce(_ + _)/x._2.size)
    
  def validateWith(deadlineSecond : Long) =
    threads.map(x => x.query -> x.validateWith(deadlineSecond)).groupBy(_._1).map(x => x._1 -> x._2.map(_._2).reduce(_ + _)/x._2.size)
  
}

object Session {
  
  private def process(file : File) = {
    val name = file.getName.dropRight(4);
    val parts = name.split("_").toList;
    println(parts)
  }
  
  
 
  
  def apply(dir : String, profileDir : String) : Session =  {
    val files = new File(dir).listFiles().toList;    
    val threads = files.map(x => Thread(Source.fromFile(x).mkString, x.getName, profileDir));
    Session(threads);
  }

  def main(args: Array[String]): Unit = {
    val nCores = 240;
    val s = Session("/home/alessandro/sessioni/5_5_6_600"+"/fetched/session", "/workspace/Query/");
    val deadline = 600;
    val qm = QueueManager(s, "queue1" -> 38.01, "queue2" ->1.94, "queue3" -> 60.05, "queue4" -> 0);
    
    qm.queues.foreach(x => println(x._2.users));
    println("Average:");
    s.validate(qm, nCores).foreach(x => println(x));
    println("Upper:")
    s.validateUpper(qm, nCores).foreach(x => println(x));
    println("Deadline:")
    s.validateWith(deadline).foreach(x => println(x));
    
  }
  
}