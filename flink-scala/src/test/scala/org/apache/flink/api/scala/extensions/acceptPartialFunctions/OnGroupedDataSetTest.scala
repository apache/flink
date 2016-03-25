package org.apache.flink.api.scala.extensions.acceptPartialFunctions

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.operators.{GroupCombineOperator, GroupReduceOperator, ReduceOperator}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions.acceptPartialFunctions.base.AcceptPartialFunctionsTestBase
import org.apache.flink.api.scala.extensions.acceptPartialFunctions.data.KeyValuePair
import org.apache.flink.api.scala.extensions.acceptPartialFunctionsOnGroupedDataSet
import org.junit.Test

class OnGroupedDataSetTest extends AcceptPartialFunctionsTestBase {

  @Test
  def testSortGroupWithOnTuple(): Unit = {
    val test =
      groupedTuples.sortGroupWith(Order.ASCENDING) {
        case (id, _) => id
      }
    assert(test.isInstanceOf[GroupedDataSet[_]],
      "sortGroupWith should produce a GroupedDataSet")
  }

  @Test
  def testSortGroupWithOnCaseClass(): Unit = {
    val test =
      groupedCaseObjects.sortGroupWith(Order.ASCENDING) {
        case KeyValuePair(id, _) => id
      }
    assert(test.isInstanceOf[GroupedDataSet[_]],
      "sortGroupWith should produce a GroupedDataSet")
  }

  @Test
  def testReduceWithOnTuple(): Unit = {
    val test =
      groupedTuples.reduceWith {
        case ((_, v1), (_, v2)) => (0, s"$v1 $v2")
      }

    assert(test.javaSet.isInstanceOf[ReduceOperator[_]],
      "reduceWith should produce a ReduceOperator")
  }

  @Test
  def testReduceWithOnCaseClass(): Unit = {
    val test =
      groupedCaseObjects.reduceWith {
        case (KeyValuePair(_, v1), KeyValuePair(_, v2)) => KeyValuePair(0, s"$v1 $v2")
      }

    assert(test.javaSet.isInstanceOf[ReduceOperator[_]],
      "reduceWith should produce a ReduceOperator")
  }

  @Test
  def testReduceGroupWithOnTuple(): Unit = {
    val accumulator: StringBuffer = new StringBuffer()
    val test =
      groupedTuples.reduceGroupWith {
        case (_, value) #:: _ => accumulator.append(value).append('\n')
      }

    assert(test.javaSet.isInstanceOf[GroupReduceOperator[_, _]],
      "reduceGroupWith should produce a GroupReduceOperator")
  }

  @Test
  def testReduceGroupWithOnCaseClass(): Unit = {
    val accumulator: StringBuffer = new StringBuffer()
    val test =
      groupedCaseObjects.reduceGroupWith {
        case KeyValuePair(_, value) #:: _ => accumulator.append(value).append('\n')
      }

    assert(test.javaSet.isInstanceOf[GroupReduceOperator[_, _]],
      "reduceGroupWith should produce a GroupReduceOperator")
  }

  @Test
  def testCombineGroupWithOnTuple(): Unit = {
    val accumulator: StringBuffer = new StringBuffer()
    val test =
      groupedTuples.combineGroupWith {
        case (_, value) #:: _ => accumulator.append(value).append('\n')
      }

    assert(test.javaSet.isInstanceOf[GroupCombineOperator[_, _]],
      "combineGroupWith should produce a GroupCombineOperator")
  }

  @Test
  def testCombineGroupWithOnCaseClass(): Unit = {
    val accumulator: StringBuffer = new StringBuffer()
    val test =
      groupedCaseObjects.combineGroupWith {
        case KeyValuePair(_, value) #:: _ => accumulator.append(value).append('\n')
      }

    assert(test.javaSet.isInstanceOf[GroupCombineOperator[_, _]],
      "combineGroupWith should produce a GroupCombineOperator")
  }

}
