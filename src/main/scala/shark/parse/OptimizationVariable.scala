package shark.parse

import java.util.{Map => JMap}
import java.util.regex.PatternSyntaxException

import scala.Array.canBuildFrom
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import shark.LogHelper
import shark.memstore2.filter._

object OptimizationVariable {
  val CACHE_FILTERS = "filters"
  val RESULT_SCHEMA = "shark.cache.result.schema"
}

class GeneralOptimizationVariable extends LogHelper {
  private[parse] val _varsMap = new HashMap[String, String]
  
  def sync(tblProperties: JMap[String, String]) {}
  
  def putVar(key: String, value: String) {
    _varsMap.put(key, value)
  }
  
  def putVarIfNotNull(key: String, value: String) {
    if (value != null) {
      _varsMap.put(key, value)
    }
  }
  
  def getVar(key: String, defaultValue: String): String = {
    _varsMap.get(key) match {
      case Some(value) => value
      case None => defaultValue
    }
  }
}

class MemoryStoreSinkOptVariable(parent: GeneralOptimizationVariable = null) 
	extends GeneralOptimizationVariable {
	if (parent != null) {
		parent._varsMap.foreach(kv => putVar(kv._1, kv._2))
	}
	
  override def sync(tblProperties: JMap[String, String]) {
    if (tblProperties != null) {
    	putVar(OptimizationVariable.CACHE_FILTERS, tblProperties.get(OptimizationVariable.CACHE_FILTERS))
    	putVarIfNotNull(OptimizationVariable.RESULT_SCHEMA, tblProperties.get(OptimizationVariable.RESULT_SCHEMA))
    }
    _varsMap.foreach(kv => (tblProperties.put(kv._1, kv._2)))
  }
  
  private def getCacheInstance(arg: String, idToColumnMap: HashMap[Int, String], 
      columnToIdMap: HashMap[String, Int]): CacheFilter = {
    val args = arg.split(":")
    var filterType = args(0)
    var argsArray = new Array[String](0)
    if (filterType.indexOf('(') != -1) {
      if (filterType.indexOf(')') == -1) {
        throw new CannotCreateFilterException("Missing ')' when parse index string : " + arg)
      }
      argsArray = filterType.substring(filterType.indexOf('(') + 1, filterType.indexOf(')')).split(',').map(_.trim)
      filterType = filterType.substring(0, filterType.indexOf('('))
    }
    val columns = arg.substring(arg.indexOf(':') + 1, arg.length)
    var cacheFilter: CacheFilter = null

    val columnsSet = new HashSet[String]
    if (!columns.isEmpty()) {
      if (columns.equals("*")) {
      	// Build filter on each columns
        columnToIdMap.foreach(kv => columnsSet.add(kv._1))
      } 
      else {
        columns.split(',').map(_.toUpperCase()).foreach(column => {
          if (!columnToIdMap.contains(column)) {
            throw new CannotCreateFilterException("Can't find column '" + column + "' in destination table")
          }
          columnsSet.add(column)
        })
      }
      if (filterType == null) {
        throw new CannotCreateFilterException("Can't find index type.")
      }
      filterType = filterType.toUpperCase()
      if (filterType.endsWith("FILTER")) {
        filterType = filterType.substring(0, filterType.length() - 6)
      }
      filterType match {
        case "UNIQUEVALUE" => 
          cacheFilter = new UniqueValueFilter(columnsSet, 
              idToColumnMap, columnToIdMap)
        case "HASHBUCKET" => 
          cacheFilter = new HashBucketFilter(columnsSet, 
              idToColumnMap, columnToIdMap, argsArray)
        case _ => throw CannotCreateFilterException.unsupportFilterTypeException(filterType)
      }
      cacheFilter
    } else {
      throw CannotCreateFilterException.cannotInitCacheFilterException(filterType)
    }
  }
  
  // Build CacheFilter for each settings
  // "filters"="FilterType1:FilterColumn1,FilterColumn2|FilterType2:FilterColumn1:..."
  def getCacheFilters: Array[CacheFilter] = {
    (_varsMap.get(OptimizationVariable.CACHE_FILTERS), _varsMap.get(OptimizationVariable.RESULT_SCHEMA)) match {
      case (Some(value), Some(resSchema)) => {
        if (value.isEmpty()) {
          return null;
        }
        val idToColumnMap = new HashMap[Int, String]
        val columnToIdMap = new HashMap[String, Int]
        resSchema.split(',').foreach(
          resStr => {
            val indexStrTypeSplits = resStr.split(':')
            val id = indexStrTypeSplits(0).toInt
            idToColumnMap.put(id, indexStrTypeSplits(1).toUpperCase)
            columnToIdMap.put(indexStrTypeSplits(1).toUpperCase, id)
          })
        value.split('|').map(getCacheInstance(_, idToColumnMap, columnToIdMap))
      }
      case _ => null
    }
  }
}

