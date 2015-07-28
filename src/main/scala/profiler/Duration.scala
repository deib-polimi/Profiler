/**
 *
 */
package profiler

/**
 * @author Alessandro
 *
 */
case class Duration(durations : Map[String, Long]) {

    val size = durations.head._1.size;

    def get(input : String) = durations(input.slice(8, 8+size));

    def contains(input : String) = durations.contains(input.slice(8, 8+size));

}

object Duration {

    def apply(text : String) : Duration = {
	    def parseEntry (text : String) : Option[(String, Long)] = {
		    val fields = text.split("\t");
		    val duration = fields(0).toLong;
		    if(duration < 0) None;
		    else Some(fields(1).substring(12) -> duration);
	    }
	    val lines = text.split("\n");
	    Duration(lines.flatMap(parseEntry).toMap);
    }

}
