/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.api.scala.stream.sql

import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.runtime.aggregate.ProcTimeBoundedProcessingOverProcessFunction
import org.apache.flink.table.functions.aggfunctions.{CountAggFunction,LongMinWithRetractAggFunction,LongSumWithRetractAggFunction}
import org.apache.flink.streaming.util.TestHarnessUtil
import org.apache.flink.types.Row
import org.apache.flink.api.java.functions.KeySelector
import java.util.Comparator
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.typeutils.ValueTypeInfo._
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import java.util.Comparator
import java.io.ObjectInputStream.GetField
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.table.api.scala.stream.sql.ProcTimeTest.{RowResultSortComparator,TupleRowSelector}
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test
import org.junit.Assert._
import org.junit._

class ProcTimeTest extends TableTestBase{
  @Test
  def testCountAggregationProcTimeHarnessPartitioned(): Unit = {
    
    val rT =  new RowTypeInfo(Array[TypeInformation[_]](
      INT_TYPE_INFO,
      LONG_TYPE_INFO,
      INT_TYPE_INFO,
      STRING_TYPE_INFO,
      LONG_TYPE_INFO),
      Array("a","b","c","d","e"))
    
    val rTA =  new RowTypeInfo(Array[TypeInformation[_]](
     LONG_TYPE_INFO), Array("count"))
      
    val processFunction = new KeyedProcessOperator[String,Row,Row](
      new ProcTimeBoundedProcessingOverProcessFunction(
        Array(new CountAggFunction),
        Array(1),
        5,
        rTA,
        1000,
        rT))
  
    val rInput:Row = new Row(5)
      rInput.setField(0, 1)
      rInput.setField(1, 11L)
      rInput.setField(2, 1)
      rInput.setField(3, "aaa")
      rInput.setField(4, 11L)
    
   val testHarness = new KeyedOneInputStreamOperatorTestHarness[String,Row,Row](
      processFunction, 
      new TupleRowSelector(3), 
      BasicTypeInfo.STRING_TYPE_INFO)
    
   testHarness.open();

   testHarness.setProcessingTime(3)

   // timestamp is ignored in processing time
    testHarness.processElement(new StreamRecord(rInput, 1001))
    testHarness.processElement(new StreamRecord(rInput, 2002))
    testHarness.processElement(new StreamRecord(rInput, 2003))
    testHarness.processElement(new StreamRecord(rInput, 2004))
   
    testHarness.setProcessingTime(1004)
  
    testHarness.processElement(new StreamRecord(rInput, 2005))
    testHarness.processElement(new StreamRecord(rInput, 2006))

    //move the timestamp to ensure the execution
    testHarness.setProcessingTime(1005)
    
    val result = testHarness.getOutput
    
    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    
    //all elements at the same proc timestamp have the same value
     val rOutput:Row = new Row(6)
      rOutput.setField(0, 1)
      rOutput.setField(1, 11L)
      rOutput.setField(2, 1)
      rOutput.setField(3, "aaa")
      rOutput.setField(4, 11L)
      rOutput.setField(5, 4L)   //count is 1
    expectedOutput.add(new StreamRecord(rOutput, 4))
    val rOutput2:Row = new Row(6)
      rOutput2.setField(0, 1)
      rOutput2.setField(1, 11L)
      rOutput2.setField(2, 1)
      rOutput2.setField(3, "aaa")
      rOutput2.setField(4, 11L)
      rOutput2.setField(5, 4L)   //count is 2 
    expectedOutput.add(new StreamRecord(rOutput2, 4))
    val rOutput3:Row = new Row(6)
      rOutput3.setField(0, 1)
      rOutput3.setField(1, 11L)
      rOutput3.setField(2, 1)
      rOutput3.setField(3, "aaa")
      rOutput3.setField(4, 11L)
      rOutput3.setField(5, 4L)   //count is 3
    expectedOutput.add(new StreamRecord(rOutput3, 4))
    val rOutput4:Row = new Row(6)
      rOutput4.setField(0, 1)
      rOutput4.setField(1, 11L)
      rOutput4.setField(2, 1)
      rOutput4.setField(3, "aaa")
      rOutput4.setField(4, 11L)
      rOutput4.setField(5, 4L)   //count is 4 
    expectedOutput.add(new StreamRecord(rOutput4, 4))
    val rOutput5:Row = new Row(6)
      rOutput5.setField(0, 1)
      rOutput5.setField(1, 11L)
      rOutput5.setField(2, 1)
      rOutput5.setField(3, "aaa")
      rOutput5.setField(4, 11L)
      rOutput5.setField(5, 2L)   //count is reset to 1 
    expectedOutput.add(new StreamRecord(rOutput5, 1005))
    val rOutput6:Row = new Row(6)
      rOutput6.setField(0, 1)
      rOutput6.setField(1, 11L)
      rOutput6.setField(2, 1)
      rOutput6.setField(3, "aaa")
      rOutput6.setField(4, 11L)
      rOutput6.setField(5, 2L)   //count is 2 
    expectedOutput.add(new StreamRecord(rOutput6, 1005))
    
    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.",
        expectedOutput, result, new RowResultSortComparator(6))
    
    testHarness.close()
        
  }
  
  @Test
  def testMinSumAggregationProcTimeHarnessPartitioned(): Unit = {
    
    val rT =  new RowTypeInfo(Array[TypeInformation[_]](
      INT_TYPE_INFO,
      LONG_TYPE_INFO,
      INT_TYPE_INFO,
      STRING_TYPE_INFO,
      LONG_TYPE_INFO),
      Array("a","b","c","d","e"))
    
    val rTA =  new RowTypeInfo(Array[TypeInformation[_]](
     LONG_TYPE_INFO), Array("count"))
      
    val processFunction = new KeyedProcessOperator[String,Row,Row](
      new ProcTimeBoundedProcessingOverProcessFunction(
        Array(new LongMinWithRetractAggFunction,new LongSumWithRetractAggFunction),
        Array(1,1),
        5,
        rTA,
        1000,
        rT))
  
    val rInput:Row = new Row(5)
      rInput.setField(0, 1)
      rInput.setField(1, 11L)
      rInput.setField(2, 1)
      rInput.setField(3, "aaa")
      rInput.setField(4, 11L)
    
   val testHarness = new KeyedOneInputStreamOperatorTestHarness[String,Row,Row](
      processFunction, 
      new TupleRowSelector(3), 
      BasicTypeInfo.STRING_TYPE_INFO)
    
   testHarness.open();

   testHarness.setProcessingTime(3)

   // timestamp is ignored in processing time
    testHarness.processElement(new StreamRecord(rInput, 1001))
    testHarness.processElement(new StreamRecord(rInput, 2002))
    testHarness.processElement(new StreamRecord(rInput, 2003))
    testHarness.processElement(new StreamRecord(rInput, 2004))
   
    testHarness.setProcessingTime(1004)
  
    testHarness.processElement(new StreamRecord(rInput, 2005))
    testHarness.processElement(new StreamRecord(rInput, 2006))

    //move the timestamp to ensure the execution
    testHarness.setProcessingTime(1005)
    
    val result = testHarness.getOutput
    
    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    
    //all elements at the same proc timestamp have the same value
     val rOutput:Row = new Row(7)
      rOutput.setField(0, 1)
      rOutput.setField(1, 11L)
      rOutput.setField(2, 1)
      rOutput.setField(3, "aaa")
      rOutput.setField(4, 11L)
      rOutput.setField(5, 11L)   //min is 11
      rOutput.setField(6, 44L)   //sum is 4*11
    expectedOutput.add(new StreamRecord(rOutput, 4))
    val rOutput2:Row = new Row(7)
      rOutput2.setField(0, 1)
      rOutput2.setField(1, 11L)
      rOutput2.setField(2, 1)
      rOutput2.setField(3, "aaa")
      rOutput2.setField(4, 11L)
      rOutput2.setField(5, 11L)   //min is 11
      rOutput2.setField(6, 44L)   //sum is 4*11
    expectedOutput.add(new StreamRecord(rOutput2, 4))
    val rOutput3:Row = new Row(7)
      rOutput3.setField(0, 1)
      rOutput3.setField(1, 11L)
      rOutput3.setField(2, 1)
      rOutput3.setField(3, "aaa")
      rOutput3.setField(4, 11L)
      rOutput3.setField(5, 11L)   //min is 11
      rOutput3.setField(6, 44L)   //sum is 4*11
    expectedOutput.add(new StreamRecord(rOutput3, 4))
    val rOutput4:Row = new Row(7)
      rOutput4.setField(0, 1)
      rOutput4.setField(1, 11L)
      rOutput4.setField(2, 1)
      rOutput4.setField(3, "aaa")
      rOutput4.setField(4, 11L)
      rOutput4.setField(5, 11L)   //min is 11
      rOutput4.setField(6, 44L)   //sum is 4*11
    expectedOutput.add(new StreamRecord(rOutput4, 4))
    val rOutput5:Row = new Row(7)
      rOutput5.setField(0, 1)
      rOutput5.setField(1, 11L)
      rOutput5.setField(2, 1)
      rOutput5.setField(3, "aaa")
      rOutput5.setField(4, 11L)
      rOutput5.setField(5, 11L)   //min is 1
      rOutput5.setField(6, 22L)   //sum is 2*11
    expectedOutput.add(new StreamRecord(rOutput5, 1005))
    val rOutput6:Row = new Row(7)
      rOutput6.setField(0, 1)
      rOutput6.setField(1, 11L)
      rOutput6.setField(2, 1)
      rOutput6.setField(3, "aaa")
      rOutput6.setField(4, 11L)
      rOutput6.setField(5, 11L)   //min is 1
      rOutput6.setField(6, 22L)   //sum is 2*11
    expectedOutput.add(new StreamRecord(rOutput6, 1005))
    
    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.",
        expectedOutput, result, new RowResultSortComparator(6))
    
    testHarness.close()
        
  }
}

object ProcTimeTest {

/*
 * Return 0 for equal Rows and non zero for different rows
 */

class RowResultSortComparator(
    private val IndexCounter:Int) extends Comparator[Object] with Serializable {
  
    override def compare( o1:Object, o2:Object):Int = {
  
      if (o1.isInstanceOf[Watermark] || o2.isInstanceOf[Watermark]) {
         0 
       } else {
        val sr0 = o1.asInstanceOf[StreamRecord[Row]]
        val sr1 = o2.asInstanceOf[StreamRecord[Row]]
        val row0 = sr0.getValue
        val row1 = sr1.getValue
        if ( row0.getArity != row1.getArity) {
          -1
        }
  
        if (sr0.getTimestamp != sr1.getTimestamp) {
          (sr0.getTimestamp() - sr1.getTimestamp())
        }

        var i = 0
        var result:Boolean = true
        while (i < IndexCounter) {
          result = (result) & (row0.getField(i)!=row1.getField(i))
          i += 1
        }
  
        if (result) {
          0
        } else {
          -1
        }
      }
   }
}

/*
 * Simple test class that returns a specified field as the selector function
 */
class TupleRowSelector(
    private val selectorField:Int) extends KeySelector[Row, String] {

  override def getKey(value: Row): String = { 
    value.getField(selectorField).asInstanceOf[String]
  }
}

}
