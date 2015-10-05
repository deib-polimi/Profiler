package profiler.session

import java.io.File

import scala.io.Source

/**
 * @author Alessandro
 */
case class Session(threads : List[Thread]) {

	def avg : List[(String, Long)] = threads.map(x => (x.fullId, x.avg));

def validate (queueManager : QueueManager, numCores : Int): Map[String, Double] =
threads.map(x => x.query -> x.validate(queueManager, numCores)).groupBy(_._1).map(x => x._1 -> x._2.map(_._2).reduce(_ + _)/x._2.size);

def validateUpper(queueManager : QueueManager, numCores : Int): Map[String, Double] =
threads.map(x => x.query -> x.validateUpper(queueManager, numCores)).groupBy(_._1).map(x => x._1 -> x._2.map(_._2).reduce(_ + _)/x._2.size);

def validateWith(deadlineSecond : Long): Map[String, Double] =
threads.map(x => x.query -> x.validateWith(deadlineSecond)).groupBy(_._1).map(x => x._1 -> x._2.map(_._2).reduce(_ + _)/x._2.size);

};

object Session {

	private def process(file : File) : Unit = {
			val name = file.getName.dropRight(4);
			val parts = name.split("_").toList;
			println(parts);
	};

	def apply (dir : File, profileDir : File) : Session =  {
			val files = dir.listFiles ().toList;
			val threads = files.map (x => Thread (Source.fromFile (x).mkString, x.getName, profileDir));
			Session(threads);
	};

  def mainEntryPoint (inputDir : File, profilesDir : File, nCores : Int,
                      deadline : Int, queues : (String, Double)*) : Unit = {
      val session = Session (inputDir, profilesDir);
      val manager = QueueManager (session, queues:_*);

      manager.queues.foreach(x => println(x._2.users));
      println("Average:");
      session.validate(manager, nCores).foreach(x => println(x));
      println("Upper:");
      session.validateUpper(manager, nCores).foreach(x => println(x));
      println("Deadline:");
      session.validateWith(deadline).foreach(x => println(x));
  }

};
