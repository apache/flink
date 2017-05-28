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
package org.apache.flink.table.runtime.aggregate

import java.util.Comparator
import java.util.concurrent.ConcurrentLinkedQueue
import java.lang.{Integer => JInt, Long => JLong}

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.{KeyedOneInputStreamOperatorTestHarness, TestHarnessUtil}
import org.apache.flink.types.Row
import org.junit.Test
import org.apache.flink.table.runtime.aggregate.ProcTimeSortProcessFunction
import org.apache.flink.table.runtime.aggregate.RowTimeSortProcessFunction
import org.apache.flink.table.runtime.aggregate.TimeSortProcessFunctionTest._
import org.apache.flink.api.java.typeutils.runtime.RowComparator
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.TypeComparator
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.streaming.api.TimeCharacteristic

class TimeSortProcessFunctionTest{

  
  @Test
  def testSortProcTimeHarnessPartitioned(): Unit = {
    
    val rT =  new RowTypeInfo(Array[TypeInformation[_]](
      INT_TYPE_INFO,
      LONG_TYPE_INFO,
      INT_TYPE_INFO,
      STRING_TYPE_INFO,
      LONG_TYPE_INFO),
      Array("a","b","c","d","e"))
    
    val rTA =  new RowTypeInfo(Array[TypeInformation[_]](
     LONG_TYPE_INFO), Array("count"))
    val indexes = Array(1,2)
      
    val fieldComps = Array[TypeComparator[AnyRef]](
      LONG_TYPE_INFO.createComparator(true, null).asInstanceOf[TypeComparator[AnyRef]],
      INT_TYPE_INFO.createComparator(false, null).asInstanceOf[TypeComparator[AnyRef]] )
    val booleanOrders = Array(true, false)    
    

    val rowComp = new RowComparator(
      rT.getTotalFields,
      indexes,
      fieldComps,
      new Array[TypeSerializer[AnyRef]](0), //used only for serialized comparisons
      booleanOrders)
    
    val collectionRowComparator = new CollectionRowComparator(rowComp)
    
    val inputCRowType = CRowTypeInfo(rT)
    
    val processFunction = new KeyedProcessOperator[Integer,CRow,CRow](
      new ProcTimeSortProcessFunction(
        inputCRowType,
        collectionRowComparator))
  
   val testHarness = new KeyedOneInputStreamOperatorTestHarness[Integer,CRow,CRow](
      processFunction, 
      new TupleRowSelector(0), 
      BasicTypeInfo.INT_TYPE_INFO)
    
   testHarness.open();

   testHarness.setProcessingTime(3)

      // timestamp is ignored in processing time
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong), true), 1001))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong), true), 2002))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong), true), 2003))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2004))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2006))

    //move the timestamp to ensure the execution
    testHarness.setProcessingTime(1005)
    
    val result = testHarness.getOutput
    
    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    
    // all elements at the same proc timestamp have the same value
    // elements should be sorted ascending on field 1 and descending on field 2
    // (10,0) (11,1) (12,2) (12,1) (12,0)
    
     expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong),true), 4))
     expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong),true), 4))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong),true), 4))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong),true), 4))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 0: JInt, "aaa", 11L: JLong),true), 4))
      
    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.",
        expectedOutput, result, new RowResultSortComparator(6))
    
    testHarness.close()
        
  }
  
  
  @Test
  def testSortProcTimeOffsetHarnessPartitioned(): Unit = {
    
    val rT =  new RowTypeInfo(Array[TypeInformation[_]](
      INT_TYPE_INFO,
      LONG_TYPE_INFO,
      INT_TYPE_INFO,
      STRING_TYPE_INFO,
      LONG_TYPE_INFO),
      Array("a","b","c","d","e"))
    
    val rTA =  new RowTypeInfo(Array[TypeInformation[_]](
     LONG_TYPE_INFO), Array("count"))
    val indexes = Array(1,2)
      
    val fieldComps = Array[TypeComparator[AnyRef]](
      LONG_TYPE_INFO.createComparator(true, null).asInstanceOf[TypeComparator[AnyRef]],
      INT_TYPE_INFO.createComparator(false, null).asInstanceOf[TypeComparator[AnyRef]] )
    val booleanOrders = Array(true, false)    
    

    val rowComp = new RowComparator(
      rT.getTotalFields,
      indexes,
      fieldComps,
      new Array[TypeSerializer[AnyRef]](0), //used only for serialized comparisons
      booleanOrders)
    
    val collectionRowComparator = new CollectionRowComparator(rowComp)
    
    val inputCRowType = CRowTypeInfo(rT)
    
    val processFunction = new KeyedProcessOperator[Integer,CRow,CRow](
      new ProcTimeSortProcessFunctionOffset(
        2,  
        inputCRowType,
        collectionRowComparator))
  
   val testHarness = new KeyedOneInputStreamOperatorTestHarness[Integer,CRow,CRow](
      processFunction, 
      new TupleRowSelector(0), 
      BasicTypeInfo.INT_TYPE_INFO)
    
   testHarness.open();

   testHarness.setProcessingTime(3)

      // timestamp is ignored in processing time
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong), true), 1001))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong), true), 2002))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong), true), 2003))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2004))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2006))

    //move the timestamp to ensure the execution
    testHarness.setProcessingTime(1005)
    
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 13L: JLong, 2: JInt, "aaa", 11L: JLong), true), 2007))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 13L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2007))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 13L: JLong, 4: JInt, "aaa", 11L: JLong), true), 2008))
    
    //move the timestamp to ensure the execution
    testHarness.setProcessingTime(1007)
    
        
    val result = testHarness.getOutput
    
    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    
    //update
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong),true), 4))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong),true), 4))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 0: JInt, "aaa", 11L: JLong),true), 4))
    //retract  
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong),false), 1006))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong),false), 1006))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 0: JInt, "aaa", 11L: JLong),false), 1006))
    //update
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 13L: JLong, 0: JInt, "aaa", 11L: JLong),true), 1006))
      
    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.",
        expectedOutput, result, new RowResultSortComparator(6))
    
    testHarness.close()
        
  }
  
  @Test
  def testSortProcTimeOffsetFetchHarnessPartitioned(): Unit = {
    
    val rT =  new RowTypeInfo(Array[TypeInformation[_]](
      INT_TYPE_INFO,
      LONG_TYPE_INFO,
      INT_TYPE_INFO,
      STRING_TYPE_INFO,
      LONG_TYPE_INFO),
      Array("a","b","c","d","e"))
    
    val rTA =  new RowTypeInfo(Array[TypeInformation[_]](
     LONG_TYPE_INFO), Array("count"))
    val indexes = Array(1,2)
      
    val fieldComps = Array[TypeComparator[AnyRef]](
      LONG_TYPE_INFO.createComparator(true, null).asInstanceOf[TypeComparator[AnyRef]],
      INT_TYPE_INFO.createComparator(false, null).asInstanceOf[TypeComparator[AnyRef]] )
    val booleanOrders = Array(true, false)    
    

    val rowComp = new RowComparator(
      rT.getTotalFields,
      indexes,
      fieldComps,
      new Array[TypeSerializer[AnyRef]](0), //used only for serialized comparisons
      booleanOrders)
    
    val collectionRowComparator = new CollectionRowComparator(rowComp)
    
    val inputCRowType = CRowTypeInfo(rT)
    
    val processFunction = new KeyedProcessOperator[Integer,CRow,CRow](
      new ProcTimeSortProcessFunctionOffsetFetch(
        2,
        2,
        inputCRowType,
        collectionRowComparator))
  
   val testHarness = new KeyedOneInputStreamOperatorTestHarness[Integer,CRow,CRow](
      processFunction, 
      new TupleRowSelector(0), 
      BasicTypeInfo.INT_TYPE_INFO)
    
   testHarness.open();

   testHarness.setProcessingTime(3)

      // timestamp is ignored in processing time
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong), true), 1001))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong), true), 2002))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong), true), 2003))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2004))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2006))

    //move the timestamp to ensure the execution
    testHarness.setProcessingTime(1005)
    
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 8L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2007))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2007))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 9L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2006))
    
    //move the timestamp to ensure the execution
    testHarness.setProcessingTime(1007)
    
    val result = testHarness.getOutput
    
    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    
    //update
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong),true), 4))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong),true), 4))
    //retract
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong),false), 1006))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong),false), 1006))
    //update
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong),true), 1006))
      
    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.",
        expectedOutput, result, new RowResultSortComparator(6))
    
    testHarness.close()
        
  }
  
  @Test
  def testSortProcTimeOffsetHarnessIdentity(): Unit = {
    
    val rT =  new RowTypeInfo(Array[TypeInformation[_]](
      INT_TYPE_INFO,
      LONG_TYPE_INFO,
      INT_TYPE_INFO,
      STRING_TYPE_INFO,
      LONG_TYPE_INFO),
      Array("a","b","c","d","e"))
    
    val inputCRowType = CRowTypeInfo(rT)
    
    val processFunction = new KeyedProcessOperator[Integer,CRow,CRow](
      new ProcTimeIdentitySortProcessFunctionOffset(
        2,  
        inputCRowType))
  
    val testHarness = new KeyedOneInputStreamOperatorTestHarness[Integer,CRow,CRow](
      processFunction, 
      new TupleRowSelector(0), 
      BasicTypeInfo.INT_TYPE_INFO)
    
    testHarness.open();

    testHarness.setProcessingTime(3)

      // timestamp is ignored in processing time
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong), true), 1001))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong), true), 2002))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong), true), 2003))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2004))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2006))

    //move the timestamp to ensure the execution
    testHarness.setProcessingTime(1005)
    
    val result = testHarness.getOutput
    
    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    
    //output is the last 3 elements (first 2 are skipped)
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong),true), 4))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 0: JInt, "aaa", 11L: JLong),true), 4))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong),true), 4))
      
    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.",
        expectedOutput, result, new RowResultSortComparator(6))
    
    testHarness.close()
        
  }
  
  
  @Test
  def testSortProcTimeOffsetFetchHarnessIdentity(): Unit = {
    
    val rT =  new RowTypeInfo(Array[TypeInformation[_]](
      INT_TYPE_INFO,
      LONG_TYPE_INFO,
      INT_TYPE_INFO,
      STRING_TYPE_INFO,
      LONG_TYPE_INFO),
      Array("a","b","c","d","e"))
    
    val inputCRowType = CRowTypeInfo(rT)
    
    val processFunction = new KeyedProcessOperator[Integer,CRow,CRow](
      new ProcTimeIdentitySortProcessFunctionOffsetFetch(
        0,
        3,
        inputCRowType))
  
    val testHarness = new KeyedOneInputStreamOperatorTestHarness[Integer,CRow,CRow](
      processFunction, 
      new TupleRowSelector(0), 
      BasicTypeInfo.INT_TYPE_INFO)
    
    testHarness.open();

    testHarness.setProcessingTime(3)

      // timestamp is ignored in processing time
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong), true), 1001))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong), true), 2002))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong), true), 2003))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2004))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2006))

    //move the timestamp to ensure the execution
    testHarness.setProcessingTime(1005)
    
    val result = testHarness.getOutput
    
    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    
    //output is the first 3 elements received
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong),true), 4))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong),true), 4))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong),true), 4))
      
    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.",
        expectedOutput, result, new RowResultSortComparator(6))
    
    testHarness.close()
        
  }
  
  
  @Test
  def testSortRowTimeHarnessPartitioned(): Unit = {
    
    val rT =  new RowTypeInfo(Array[TypeInformation[_]](
      INT_TYPE_INFO,
      LONG_TYPE_INFO,
      INT_TYPE_INFO,
      STRING_TYPE_INFO,
      LONG_TYPE_INFO),
      Array("a","b","c","d","e"))
    
    val rTA =  new RowTypeInfo(Array[TypeInformation[_]](
     LONG_TYPE_INFO), Array("count"))
    val indexes = Array(1,2)
      
    val fieldComps = Array[TypeComparator[AnyRef]](
      LONG_TYPE_INFO.createComparator(true, null).asInstanceOf[TypeComparator[AnyRef]],
      INT_TYPE_INFO.createComparator(false, null).asInstanceOf[TypeComparator[AnyRef]] )
    val booleanOrders = Array(true, false)    
    

    val rowComp = new RowComparator(
      rT.getTotalFields,
      indexes,
      fieldComps,
      new Array[TypeSerializer[AnyRef]](0), //used only for serialized comparisons
      booleanOrders)
    
    val collectionRowComparator = new CollectionRowComparator(rowComp)
    
    val inputCRowType = CRowTypeInfo(rT)
    
    val processFunction = new KeyedProcessOperator[Integer,CRow,CRow](
      new RowTimeSortProcessFunction(
        inputCRowType,
        collectionRowComparator))
  
   val testHarness = new KeyedOneInputStreamOperatorTestHarness[Integer,CRow,CRow](
      processFunction, 
      new TupleRowSelector(0), 
      BasicTypeInfo.INT_TYPE_INFO)
    
   testHarness.open();

   testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime)
   testHarness.processWatermark(3)

      // timestamp is ignored in processing time
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong), true), 1001))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong), true), 2002))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong), true), 2002))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 11L: JLong), true), 2004))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2006))

    //move the timestamp to ensure the execution
    testHarness.processWatermark(2007)
    
    val result = testHarness.getOutput
    
    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    
    // all elements at the same proc timestamp have the same value
    // elements should be sorted ascending on field 1 and descending on field 2
    // (10,0) (11,1) (12,2) (12,1) (12,0)
    expectedOutput.add(new Watermark(3))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong),true), 1001))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong),true), 2002))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong),true), 2002))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 11L: JLong),true), 2004))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong),true), 2006))
    expectedOutput.add(new Watermark(2007))
    
    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.",
        expectedOutput, result, new RowResultSortComparator(6))
        
    testHarness.close()
        
  }
  
  @Test
  def testSortRowTimeOffsetHarnessPartitioned(): Unit = {
    
    val rT =  new RowTypeInfo(Array[TypeInformation[_]](
      INT_TYPE_INFO,
      LONG_TYPE_INFO,
      INT_TYPE_INFO,
      STRING_TYPE_INFO,
      LONG_TYPE_INFO),
      Array("a","b","c","d","e"))
    
    val rTA =  new RowTypeInfo(Array[TypeInformation[_]](
     LONG_TYPE_INFO), Array("count"))
    val indexes = Array(1,2)
      
    val fieldComps = Array[TypeComparator[AnyRef]](
      LONG_TYPE_INFO.createComparator(true, null).asInstanceOf[TypeComparator[AnyRef]],
      INT_TYPE_INFO.createComparator(false, null).asInstanceOf[TypeComparator[AnyRef]] )
    val booleanOrders = Array(true, false)    
    

    val rowComp = new RowComparator(
      rT.getTotalFields,
      indexes,
      fieldComps,
      new Array[TypeSerializer[AnyRef]](0), //used only for serialized comparisons
      booleanOrders)
    
    val collectionRowComparator = new CollectionRowComparator(rowComp)
    
    val inputCRowType = CRowTypeInfo(rT)
    
    val processFunction = new KeyedProcessOperator[Integer,CRow,CRow](
      new RowTimeSortProcessFunctionOffset(
        2,
        inputCRowType,
        collectionRowComparator))
  
   val testHarness = new KeyedOneInputStreamOperatorTestHarness[Integer,CRow,CRow](
      processFunction, 
      new TupleRowSelector(0), 
      BasicTypeInfo.INT_TYPE_INFO)
    
   testHarness.open();

   testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime)
   testHarness.processWatermark(3)

      // timestamp is ignored in processing time
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong), true), 1001))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong), true), 2002))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong), true), 2002))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 11L: JLong), true), 2002))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2002))

    //move the timestamp to ensure the execution
    testHarness.processWatermark(2007)
    
    val result = testHarness.getOutput
    
    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    
    expectedOutput.add(new Watermark(3))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong),true), 2002))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong),true), 2002))
    expectedOutput.add(new Watermark(2007))
    
    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.",
        expectedOutput, result, new RowResultSortComparator(6))
        
    testHarness.close()
        
  }
  
  @Test
  def testSortRowTimeOffsetFetchHarnessPartitioned(): Unit = {
    
    val rT =  new RowTypeInfo(Array[TypeInformation[_]](
      INT_TYPE_INFO,
      LONG_TYPE_INFO,
      INT_TYPE_INFO,
      STRING_TYPE_INFO,
      LONG_TYPE_INFO),
      Array("a","b","c","d","e"))
    
    val rTA =  new RowTypeInfo(Array[TypeInformation[_]](
     LONG_TYPE_INFO), Array("count"))
    val indexes = Array(1,2)
      
    val fieldComps = Array[TypeComparator[AnyRef]](
      LONG_TYPE_INFO.createComparator(true, null).asInstanceOf[TypeComparator[AnyRef]],
      INT_TYPE_INFO.createComparator(false, null).asInstanceOf[TypeComparator[AnyRef]] )
    val booleanOrders = Array(true, false)    
    

    val rowComp = new RowComparator(
      rT.getTotalFields,
      indexes,
      fieldComps,
      new Array[TypeSerializer[AnyRef]](0), //used only for serialized comparisons
      booleanOrders)
    
    val collectionRowComparator = new CollectionRowComparator(rowComp)
    
    val inputCRowType = CRowTypeInfo(rT)
    
    val processFunction = new KeyedProcessOperator[Integer,CRow,CRow](
      new RowTimeSortProcessFunctionOffsetFetch(
        0,
        3,
        inputCRowType,
        collectionRowComparator))
  
   val testHarness = new KeyedOneInputStreamOperatorTestHarness[Integer,CRow,CRow](
      processFunction, 
      new TupleRowSelector(0), 
      BasicTypeInfo.INT_TYPE_INFO)
    
   testHarness.open();

   testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime)
   testHarness.processWatermark(3)

      // timestamp is ignored in processing time
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong), true), 1001))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong), true), 2002))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong), true), 2002))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 11L: JLong), true), 2002))
    testHarness.processElement(new StreamRecord(new CRow(
        Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2002))

    //move the timestamp to ensure the execution
    testHarness.processWatermark(2007)
    
    val result = testHarness.getOutput
    
    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    
    expectedOutput.add(new Watermark(3))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong),true), 1001))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong),false), 2002))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong),true), 2002))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 11L: JLong),true), 2002))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong),true), 2002))
    expectedOutput.add(new Watermark(2007))
    
    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.",
        expectedOutput, result, new RowResultSortComparator(6))
        
    testHarness.close()
        
  }
  
}

object TimeSortProcessFunctionTest{

/**
 * Return 0 for equal Rows and non zero for different rows
 */
class RowResultSortComparator(indexCounter: Int) extends Comparator[Object] with Serializable {

    override def compare(o1: Object, o2: Object):Int = {

      if (o1.isInstanceOf[Watermark] || o2.isInstanceOf[Watermark]) {
        // watermark is not expected
         -1
       } else {
        val row1 = o1.asInstanceOf[StreamRecord[CRow]].getValue
        val row2 = o2.asInstanceOf[StreamRecord[CRow]].getValue
        row1.toString.compareTo(row2.toString)
      }
   }
}

/**
 * Simple test class that returns a specified field as the selector function
 */
class TupleRowSelector(
    private val selectorField:Int) extends KeySelector[CRow, Integer] {

  override def getKey(value: CRow): Integer = {
    value.row.getField(selectorField).asInstanceOf[Integer]
  }
}

}
