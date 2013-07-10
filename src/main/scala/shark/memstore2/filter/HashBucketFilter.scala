package shark.memstore2.filter

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils

/* Only support one column */
class HashBucketFilter(
    keyColumns : HashSet[String], 
    idToColumnMap : HashMap[Int, String],
    columnToIdMap : HashMap[String, Int],
    argsArray: Array[String]) 
    extends CacheFilter(keyColumns, idToColumnMap, columnToIdMap, argsArray)  {
  var hash: Int = _
  var bucketSize: Int = _
  var hasInit: Boolean = false
  var columnType = FilterValueType.None
  if (keyColumns.size > 1) {
    throw new CannotCreateFilterException("HashIndex can't support multi column")
  }
  if (argsArray.size != 1) {
    throw new CannotCreateFilterException("HashIndex need bucket size as arguments")
  }
  try {
    bucketSize = argsArray(0).toInt
  }
  catch {
  	case e: Exception => throw new CannotCreateFilterException("Cannot parse " + argsArray(0) + " to Int")
  }
  
  override def buildIndex(colname: String, hiveValue: Object, oi: ObjectInspector) = {
    if (keyColumns != null) {
      if (keyColumns.contains(colname.toUpperCase)) {
        val valueHashCode = ObjectInspectorUtils.hashCode(hiveValue, oi)
        if (!hasInit) {
          hasInit = true
          columnType = CacheFilter.getColumnType(
              ObjectInspectorUtils.copyToStandardJavaObject(hiveValue, oi))
          if (columnType == FilterValueType.None) {
            throw new CannotCreateFilterException("Can't create filter on value " 
                + hiveValue.toString + " value type is " + hiveValue.getClass().toString)
          }
          hash = valueHashCode % bucketSize
          logInfo("Create index on column " 
              + colname.toUpperCase + " hash : " 
              + valueHashCode + " bucket id : " + hash)
        }
        else {
          if (hash != (valueHashCode % bucketSize)) {
            throw new CannotCreateFilterException("Different hash code, expect " + hash + " but " + (valueHashCode % bucketSize) + " has found")
          }
        }
      }
    }
  }
  
  override def test(colname : String, value : Any, opt : Int) : Boolean = {
    if (!hasInit) {
      false
    }
    else {
      if (opt == CacheFilter.OptEqual) {
      	CacheFilter.tryCompareHashCode(columnType, value, bucketSize, hash) == 0
      }
      else {
      	true
      }
    }
  }
  
  override def testIn(colname : String, values : List[Any]) : Boolean = {
    if (!hasInit) {
      false
    }
    else {
      var hasEqual = false
      values.foreach(value => 
        if (!hasEqual) hasEqual = 
          (CacheFilter.tryCompareHashCode(columnType, value, bucketSize, hash) == 0))
      hasEqual
  	}
  }
  
  override def testBetweenAnd(colname : String, val1 : Any, val2 : Any) : Boolean = {
    if (!hasInit) {
      false
    }
    else {
    	true
    }
  }
}