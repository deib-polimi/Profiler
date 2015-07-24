/**
 *
 */
package mapreduce

/**
 * @author Alessandro
 *
 */
sealed abstract class TaskType;

case object MAP extends TaskType;

case object REDUCE extends TaskType;

case object SHUFFLE extends TaskType; 