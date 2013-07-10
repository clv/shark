package shark

import java.lang.Integer

import org.scalatest.FunSuite
import shark.memstore2.filter._
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap

class CacheFilterSuite extends FunSuite {
  
  val tableColumn = new HashSet[String]
  val tableIdToColumn = new HashMap[Int, String]
  val tableColumnToId = new HashMap[String, Int]
  
  tableColumn += "COL"
  tableIdToColumn += ((0, "COL"))
  tableColumnToId += (("COL", 0))
  
	test("Test UniqueValueFilter") {
	  val filter = new UniqueValueFilter(
	      tableColumn, 
	      tableIdToColumn,
	      tableColumnToId)
	  /* Number */
	  filter.indexMap.put("COL", new Integer(1))
	  assert(filter.test("COL", 1, CacheFilter.OptEqual) == true)
	  assert(filter.test("COL", 2, CacheFilter.OptEqual) == false)
	  assert(filter.test("COL", 2, CacheFilter.OptLess) ==  true)
	  assert(filter.test("COL", 1, CacheFilter.OptLess) == false)
	  assert(filter.test("COL", 0, CacheFilter.OptMore) == true)
	  assert(filter.test("COL", 1, CacheFilter.OptMore) == false)
	  assert(filter.test("COL", 2, CacheFilter.OptLessEqual) == true)
	  assert(filter.test("COL", 1, CacheFilter.OptLessEqual) == true)
	  assert(filter.test("COL", 0, CacheFilter.OptLessEqual) == false)
	  assert(filter.test("COL", 0, CacheFilter.OptMoreEqual) == true)
	  assert(filter.test("COL", 1, CacheFilter.OptMoreEqual) == true)
	  assert(filter.test("COL", 2, CacheFilter.OptMoreEqual) == false)
	  
	  assert(filter.testBetweenAnd("COL", 1, 2) == true)
	  assert(filter.testBetweenAnd("COL", 2, 3) == false)
	  assert(filter.testBetweenAnd("COL", 0, 1) == true)
	  assert(filter.testBetweenAnd("COL", -2, 0) == false)
	  
	  assert(filter.testIn("COL", List(0, 1, 2, 3)) == true)
	  assert(filter.testIn("COL", List(0, 2, 3)) == false)
	  
	  
	  /* String */
	  filter.indexMap.put("COL", "1")
	  assert(filter.test("COL", "1", CacheFilter.OptEqual) == true)
	  assert(filter.test("COL", "2", CacheFilter.OptEqual) == false)
	  assert(filter.test("COL", "2", CacheFilter.OptLess) ==  true)
	  assert(filter.test("COL", "1", CacheFilter.OptLess) == false)
	  assert(filter.test("COL", "0", CacheFilter.OptMore) == true)
	  assert(filter.test("COL", "1", CacheFilter.OptMore) == false)
	  assert(filter.test("COL", "2", CacheFilter.OptLessEqual) == true)
	  assert(filter.test("COL", "1", CacheFilter.OptLessEqual) == true)
	  assert(filter.test("COL", "0", CacheFilter.OptLessEqual) == false)
	  assert(filter.test("COL", "0", CacheFilter.OptMoreEqual) == true)
	  assert(filter.test("COL", "1", CacheFilter.OptMoreEqual) == true)
	  assert(filter.test("COL", "2", CacheFilter.OptMoreEqual) == false)
	  
	  assert(filter.testBetweenAnd("COL", "1", "2") == true)
	  assert(filter.testBetweenAnd("COL", "2", "3") == false)
	  assert(filter.testBetweenAnd("COL", "0", "1") == true)
	  assert(filter.testBetweenAnd("COL", "0", "0") == false)
	  
	  assert(filter.testIn("COL", List("0", "1", "2", "3")) == true)
	  assert(filter.testIn("COL", List("0", "2", "3")) == false)
	  
	  /* Test SupportNot() */
	  assert(filter.supportNOT == true)
  }
  
  test("Test HashBucketFilter") {
     val filter = new HashBucketFilter (
	      tableColumn, 
	      tableIdToColumn,
	      tableColumnToId,
	      Array("4"))
     /* Number */
     filter.hash = 1
     filter.hasInit = true
     filter.columnType = CacheFilter.getColumnType(new Integer(1))
     
     assert(filter.columnType == FilterValueType.Number)
     
     assert(filter.test("COL", 5, CacheFilter.OptEqual) == true)
     assert(filter.test("COL", 6, CacheFilter.OptEqual) == false)
     assert(filter.test("COL", 1, CacheFilter.OptLess) == true)
     assert(filter.test("COL", 1, CacheFilter.OptLessEqual) == true)
     assert(filter.test("COL", 1, CacheFilter.OptMore) == true)
     assert(filter.test("COL", 1, CacheFilter.OptMoreEqual) == true)
     
     assert(filter.testBetweenAnd("COL", 4, 5) == true)
     
     assert(filter.testIn("COL", List(2, 3, 4, 5)) == true)
     assert(filter.testIn("COL", List(2, 3, 4)) == false)
     
     /* String */
     filter.hash = "1".hashCode() % 4
     filter.hasInit = true
     filter.columnType = CacheFilter.getColumnType("1")
     
     assert(filter.columnType == FilterValueType.String)
     
     assert(filter.test("COL", "5", CacheFilter.OptEqual) == true)
     assert(filter.test("COL", "6", CacheFilter.OptEqual) == false)
     assert(filter.test("COL", "1", CacheFilter.OptLess) == true)
     assert(filter.test("COL", "1", CacheFilter.OptLessEqual) == true)
     assert(filter.test("COL", "1", CacheFilter.OptMore) == true)
     assert(filter.test("COL", "1", CacheFilter.OptMoreEqual) == true)
     
     assert(filter.testBetweenAnd("COL", "4", "5") == true)
     
     assert(filter.testIn("COL", List("2", "3", "4", "5")) == true)
     assert(filter.testIn("COL", List("2", "3", "4")) == false)
     
     
     /* Test SupportNot() */
	   assert(filter.supportNOT == false)
  }
}