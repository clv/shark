package shark.memstore2.filter

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils

/**
 * Create a index from source table, 
 * only can create when the source table has been partitioned, 
 * and no shuffle happened in this query
 * Only support static partition from sources table now.
 * 
 */
class UniqueValueFilter(
    keyColumns: HashSet[String], 
    idToColumnMap: HashMap[Int, String],
    columnToIdMap: HashMap[String, Int]) 
    extends CacheFilter(keyColumns, idToColumnMap
        , columnToIdMap) {
  val indexMap : HashMap[String, Object] = new HashMap[String, Object]
  /*
   * Here is a bug (Has been fixed)
   * If value is not String(like Long as org.apache.hadoop.hive.serde2.lazy.LazyLong)
   * Spark will try to serialize this object onto the whole cluster,
   * but LazyLong in hive is NOT a serializable object.  
   */
  
  override def supportNOT(): Boolean = true
  
  override def buildIndex(colname: String, hiveValue: Object, oi: ObjectInspector) = {
    if (keyColumns != null) {
      if (keyColumns.contains(colname.toUpperCase())) {
        val indexValue = indexMap.get(colname.toUpperCase)
//        val value = CacheIndex.lazyPrimitive2Serializable(hiveValue)
        val value = ObjectInspectorUtils.copyToStandardJavaObject(hiveValue, oi);
        if (value == null) {
          throw CannotCreateFilterException.filterCannotBeNullException
        }
        if (indexValue == None) {
          indexMap.put(colname.toUpperCase, value)
          logInfo("Create index on column " 
              + colname.toUpperCase +  " value " + value.toString + 
              "(" + (value.getClass.getName) + ")")
        } else {
          if (!indexValue.get.equals(value)) {
//            logWarningOnce("Create narrow dependency on different values.")
            throw CannotCreateFilterException.inconsistentColumnPartitionValueException
          }
        }
      }
    }
	}
	
	override def test(colname : String, value : Any, opt : Int) : Boolean = {
	  indexMap.get(colname.toUpperCase) match {
      case Some(partsVal) =>
        CacheFilter.tryCompare(partsVal, value) match {
          case Some(compareRes) =>
            val res: Boolean = opt match {
              case CacheFilter.OptEqual => compareRes == 0
              case CacheFilter.OptLess => compareRes < 0
              case CacheFilter.OptMore => compareRes > 0
              case CacheFilter.OptLessEqual => compareRes <= 0
              case CacheFilter.OptMoreEqual => compareRes >= 0
              case _ => true
            }
            res
          case _ => true
        }
	    case _ => true
	  }
	}
	
	override def testIn(colname : String, values : List[Any]) : Boolean = {
	  indexMap.get(colname.toUpperCase) match {
	    case Some(partsVal) => {
	      var res = false
	      values.foreach(value => 
	        if (!res) {
	          res = CacheFilter.tryCompare(partsVal, value) match {
	            case Some(cmpVal) => cmpVal == 0
	            case _ => true
	          }
	        })
	      res
	    }
	    case _ => true
	  }
	}
	
	override def testBetweenAnd(colname : String, val1 : Any, val2 : Any) : Boolean = {
	  indexMap.get(colname.toUpperCase) match {
	    case Some(partsVal) => {
	      // Must val1 <= partsVal <= val2
	      val res1 = CacheFilter.tryCompare(partsVal, val1)
	      val res2 = CacheFilter.tryCompare(partsVal, val2)
	      if (res1 != None && res2 != None) {
	        res1.get >= 0 && res2.get <= 0
	      }
	      else {
	        true
	      }
	    }
	    case _ => true
	  }
	}
}