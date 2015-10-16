package session

import java.text.SimpleDateFormat
import java.util.Locale

/**
 * @author alessandro
 */
case class Run(name : String, start : Long, duration : Long) {}

object Run{

  private def parseData(input : String) =
    new SimpleDateFormat("d HH:mm:ss.SSS", Locale.ENGLISH).parse(input).getTime

  def apply(text : String) : Run = {
    val fields = text.split(",").map(x => x.trim()).toList
    val start = parseData(fields(1))
    val stop = parseData(fields(2))
    Run(fields.head, start, stop-start)
  }

}
