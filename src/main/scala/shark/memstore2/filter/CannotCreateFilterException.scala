package shark.memstore2.filter

class CannotCreateFilterException(val msg : String = "") extends Exception(msg) 
	with Serializable {
  
}

object CannotCreateFilterException extends Exception {
  /*
   * Throw this exception when use SingleValueFilter, 
   * happened when the values of the partition key in one split are differences. 
   */
  def inconsistentColumnPartitionValueException() : CannotCreateFilterException = {
    new CannotCreateFilterException("Inconsistent partition value")
  }
  
  def cannotInitCacheFilterException(typeStr : String) : CannotCreateFilterException = {
    new CannotCreateFilterException("Can't init filter " + typeStr)
  }
  
  /*
   * Un-support create key on non-primary type column 
   */
  def unsupportColumnTypeException() : CannotCreateFilterException = {
    new CannotCreateFilterException("Cannot create filter on non-primary type column now")
  }
  
  /*
   * Can't find the cache class to match the cache type
   */
  def unsupportFilterTypeException(filterType: String) : CannotCreateFilterException = {
    new CannotCreateFilterException("Can't create filter: " + filterType)
  }
  
  /*
   * Filter value can't be null
   */
  def filterCannotBeNullException() : CannotCreateFilterException = {
    new CannotCreateFilterException("Filter value can't be null")
  }
}