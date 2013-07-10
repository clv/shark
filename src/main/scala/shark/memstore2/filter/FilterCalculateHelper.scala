package org.apache.hadoop.hive.ql.exec

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr

import shark.memstore2.filter.CacheFilter
import shark.memstore2.filter.EmptyPartitionCacheFilter
import shark.memstore2.TablePartitionStats


object IndexCalculateHelper {
  
  def testAllIndexes(s: TablePartitionStats, e: ExprNodeEvaluator): Boolean = {
    if (s.indexes == null) {
      true
    }
    else {
      var res = true
      s.indexes.foreach(index => {
        if (res) {
          if (!(test(index, e))) {
            res = false
          }
        }
      })
      res
    }
  }
  
  private def test(index:CacheFilter, e: ExprNodeEvaluator): Boolean = {
    if (index.isInstanceOf[EmptyPartitionCacheFilter]) {
      false
    } 
    else {
      e match {
        case e: ExprNodeGenericFuncEvaluator => {
          e.genericUDF match {
            case _: GenericUDFOPAnd => test(index, e.children(0)) && test(index, e.children(1))
            case _: GenericUDFOPOr => test(index, e.children(0)) || test(index, e.children(1))
            case _: GenericUDFOPNot => {
              if (index.supportNOT) {
                !test(index, e.children(0))
              }
              else {
                true
              }
            } 
            case udf: GenericUDFBaseCompare =>
              testComparisonPredicate(index, udf, e.children(0), e.children(1))
            case udfIn: GenericUDFIn =>
              testIn(index, udfIn, e.children)
            case udfBetween: GenericUDFBetween =>
              testBetween(index, udfBetween, e.children)
            case _ => true
          }
        }
        case _ => true
      }
    }
  }

	private def testIn(index: CacheFilter,
    udf: GenericUDFIn,
    children: Array[ExprNodeEvaluator]): Boolean = {
	  val left = children(0)
	  val rights = new ArrayBuffer[Any]
	  // In UDFIn left parameters must be a column 
	  val columnEval: ExprNodeColumnEvaluator =  
	   if (left.isInstanceOf[ExprNodeColumnEvaluator])
       left.asInstanceOf[ExprNodeColumnEvaluator]
	   else 
	  	 null
	  	 
	  // Do the right parameters
	  children.filter(_.isInstanceOf[ExprNodeConstantEvaluator])
	  	.foreach(e => {
	  	  val constVal = e.asInstanceOf[ExprNodeConstantEvaluator]
	  	  rights += constVal.expr.getValue()
	  	})
	  	 
	  if (left != null && rights.size == children.size - 1) {
      val field = columnEval.field
      val colname = field.getFieldName().toUpperCase
      if (index.keyColumns.contains(colname.toUpperCase())) {
        index.testIn(colname, rights.toList)
      } else {
        true
      }
    }
	  else {
	    true
	  }
	}
	
	private def testBetween(index: CacheFilter,
    udf: GenericUDFBetween,
    children: Array[ExprNodeEvaluator]): Boolean = {
	  // Child 0 must be the const value[Boolean]
	  val isNot : Boolean = children(0).asInstanceOf[ExprNodeConstantEvaluator].
	  		expr.getValue().asInstanceOf[java.lang.Boolean]
	  val left = children(1)
	  val min = children(2)
	  val max = children(3)
	  val columnEval: ExprNodeColumnEvaluator =  
	   if (left.isInstanceOf[ExprNodeColumnEvaluator])
       left.asInstanceOf[ExprNodeColumnEvaluator]
	   else 
	  	 null
	  	 
	  val constMinEval : ExprNodeConstantEvaluator =
      if (min.isInstanceOf[ExprNodeConstantEvaluator])
        min.asInstanceOf[ExprNodeConstantEvaluator]
      else null
    val constMaxEval : ExprNodeConstantEvaluator = 
      if (max.isInstanceOf[ExprNodeConstantEvaluator])
        max.asInstanceOf[ExprNodeConstantEvaluator]
      else null
    
    if (columnEval != null && constMinEval != null && constMaxEval != null) {
      	    val field = columnEval.field
      val colname = field.getFieldName().toUpperCase()
	    if (index.keyColumns.contains(colname)) {
	      var val1 = constMinEval.expr.getValue()
	      var val2 = constMaxEval.expr.getValue()
        CacheFilter.tryCompare(val1, val2) match {
          case Some(cmp) => {
          	/* Swap val1 and val2 when val1 > val2 */
            if (cmp > 0) {
              var tmp = val1
              val1 = val2
              val2 = tmp
            }
            var res = index.testBetweenAnd(colname, val1, val2)
            if (isNot) {
              !res
            } else {
              res
            }
          } 
	        case _ => true
	      }
	    }
      else
	      true
    }
    else 
    	true
	}
	
	private def testComparisonPredicate(
    index: CacheFilter,
    udf: GenericUDFBaseCompare,
    left: ExprNodeEvaluator,
    right: ExprNodeEvaluator): Boolean = {

    // Try to get the column evaluator.
    val columnEval: ExprNodeColumnEvaluator =
      if (left.isInstanceOf[ExprNodeColumnEvaluator])
        left.asInstanceOf[ExprNodeColumnEvaluator]
      else if (right.isInstanceOf[ExprNodeColumnEvaluator])
        right.asInstanceOf[ExprNodeColumnEvaluator]
      else null

    // Try to get the constant value.
    val constEval: ExprNodeConstantEvaluator =
      if (left.isInstanceOf[ExprNodeConstantEvaluator])
        left.asInstanceOf[ExprNodeConstantEvaluator]
      else if (right.isInstanceOf[ExprNodeConstantEvaluator])
        right.asInstanceOf[ExprNodeConstantEvaluator]
      else null

    if (columnEval != null && constEval != null) {
      // We can prune the partition only if it is a predicate of form
      //  column op const, where op is <, >, =, <=, >=.
      val field = columnEval.field
      val value: Object = constEval.expr.getValue
      val colname = field.getFieldName().toUpperCase()
      if (index.keyColumns.contains(colname)) {
        udf match {
          case _: GenericUDFOPEqual => index.test(colname, value, CacheFilter.OptEqual)
          case _: GenericUDFOPEqualOrGreaterThan => index.test(colname, value, CacheFilter.OptMoreEqual)
          case _: GenericUDFOPEqualOrLessThan => index.test(colname, value, CacheFilter.OptLessEqual)
          case _: GenericUDFOPGreaterThan => index.test(colname, value, CacheFilter.OptMore)
          case _: GenericUDFOPLessThan => index.test(colname, value, CacheFilter.OptLess)
          case _: GenericUDFOPNotEqual => {
            if (index.supportNOT) {
              !index.test(colname, value, CacheFilter.OptEqual)
            } else {
              true
            }
          }
          case _ => true
        }
      } else {
        true
      }
    } else {
      // If the predicate is not of type column op value, don't prune.
      true
    }
  }
}