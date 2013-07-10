package shark.memstore2.filter

import java.lang.{Boolean => JBoolean}
import java.lang.{Byte => JByte}
import java.lang.{Double => JDouble}
import java.lang.{Float => JFloat}
import java.lang.{Integer => JInteger}
import java.lang.{Long => JLong}

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io.Text

import spark.Logging

/** 
 * Create Index for Partition in shark cache now
 * Use TableStats to put this index.
 * This index will build dynamic during evaluation.
 * In current version we don't care any schema and tbl properties, 
 * we are only interesting to value.
 * 
 * In first version, we can have three index.
 * 1. NarrowPartitionIndex for the table which has been partitioned.
 *    Limit : RDD cann't be shuffle during loading at all.
 * 2. HFilePartitionIndex to filter RDD by rowkey.
 * 3. ShufflePartitionIndex for any key(s), of course we should do an shuffle operator before.
 */

abstract class CacheFilter
	(var keyColumns: HashSet[String], 
	 var idToColumnMap: HashMap[Int, String],
	 var columnToIdMap: HashMap[String, Int],
	 var argsArray: Array[String] = new Array[String](0)) 
	extends java.io.Serializable with Logging {
  /* Build index interface */
  val msgSet = new HashSet[String]
	def buildIndex(colName: String, value: Object, oi: ObjectInspector)
	
	/* Do filter interface, must support comparable interface */
	def test(colname : String, value : Any, opt : Int): Boolean
	/* key in (val1, val2, val3, ...) */
	def testIn(colname : String, values : List[Any]): Boolean = true
	/* key between min and max */
	def testBetweenAnd(colname : String, val1 : Any, val2 : Any): Boolean = true
	
	/* Filter Attribute Set */
	def supportNOT(): Boolean = false
	
	def logWarningOnce(msg : String) {
    if (!msgSet.contains(msg)) {
      logWarning(msg)
      msgSet += msg
    } 
  }
  
  def logInfoOnce(msg : String) {
    if (!msgSet.contains(msg)) {
      logInfo(msg)
      msgSet += msg
    } 
  }
}

class EmptyPartitionCacheFilter extends CacheFilter(null, null, null) {
    override def buildIndex(colName: String, value: Object, oi: ObjectInspector) {}
    override def test(colName: String, value: Any, opt: Int) : Boolean = {false}
    override def testIn(colName: String, values: List[Any]) : Boolean = {false}
    override def testBetweenAnd(colname: String, val1: Any, val2: Any) : Boolean = {false}
}

object FilterValueType extends Enumeration {
  type FilterValueType = Value
  val None,Number,String,Boolean,Text = Value
}

object CacheFilter {
  val OptLess = 0
  val OptLessEqual = 1
  val OptEqual = 2
  val OptMoreEqual = 3
  val OptMore = 4

 
  def getColumnType(obj: Any): FilterValueType.Value = {
    obj match {
      case x: Number => FilterValueType.Number
      case x: String => FilterValueType.String
      case x: Text => FilterValueType.Text
      case x: Boolean => FilterValueType.Boolean
      case _ => FilterValueType.None
    }
  }
  
  def hashCode(valueType: FilterValueType.Value, obj: Any): Option[Int] = {
        valueType match {
      case FilterValueType.Number => {
        obj match {
          case obj: Number => Some(obj.hashCode)
          case _ => None
        }
      }
      case FilterValueType.Boolean => {
        obj match {
          case obj: Boolean => Some(obj.hashCode)
          case _ => None
        }
      }
      case FilterValueType.Text => {
        obj match {
          case obj: Text => Some(obj.hashCode)
          case obj: String => Some((new Text(obj)).hashCode)
          case _ => None
        }
      }
      case FilterValueType.String => {
        obj match {
          case obj: Text => Some(obj.toString().hashCode)
          case obj: String => Some(obj.hashCode)
          case obj: Number => Some(obj.toString().hashCode)
          case _ => None
        }
      }
      case _ => None
    }
  }
  
  def tryCompareHashCode(valueType: FilterValueType.Value, obj: Any
      , bucketSize: Int, hash: Int): Int = {
  		hashCode(valueType, obj) match {
  		  case Some(x: Int) => {
  		    hash - x % bucketSize
  		  }
  		  case _ => 0
  		}
  }
  
  /**
   * Try to compare value a and b.
   * If a is greater than b, return 1.
   * If a equals b, return 0.
   * If a is less than b, return -1.
   * If a and b are not comparable, return None.
   */
  def tryCompare(a: Any, b: Any): Option[Int] = {
    a match {
      case a: Number => b match {
        case b: Number => Some((a.longValue - b.longValue).toInt)
        case _ => None
      }
      case a: Boolean => b match {
        case b: Boolean => Some(if (a && !b) 1 else if (!a && b) -1 else 0)
        case _ => None
      }
      case a: Text => b match {
        case b: Text => Some(a.compareTo(b))
        case b: String => Some(a.compareTo(new Text(b)))
        case _ => None
      }
      /*
       * In NarrowDependency index a is always String, because we create index
       * in CTAS.
       */
      case a: String => b match {
        case b: Text => Some((new Text(a)).compareTo(b))
        case b: String => Some(a.compareTo(b))
        case b: Number => Some((JLong.valueOf(a) - b.longValue()).toInt)
        case _ => None
      }
      case _ => None
    }
  }
}