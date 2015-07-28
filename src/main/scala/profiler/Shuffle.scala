package profiler

/**
 * @author Alessandro
 */
case class Shuffle(shuffles : Map[String, Record]) {
    println("SHUFFLE!")

    def apply (data : Array[Record]) : Array[Record] = {
	    val maps = data.filter(_.taskType == MAP);
	    val reduces = data.filter(_.taskType == REDUCE).map(x => x - this.shuffles(x.name));
	    val shuffles = reduces.map(x => this.shuffles(x.name));
	    maps ++ reduces ++ shuffles;
    }

}

object Shuffle{

    def apply(input : String) : Shuffle = {
	    val entries = input.split("\n\n").flatMap(_.split("\n")).map(Record(_).asShuffle).toSeq;
	    Shuffle(entries.map(x => (x.name, x)).toMap);
    }

}
