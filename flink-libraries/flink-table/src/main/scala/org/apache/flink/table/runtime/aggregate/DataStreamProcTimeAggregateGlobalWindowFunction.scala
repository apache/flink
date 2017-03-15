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

import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.functions.Accumulator

import java.lang.Iterable
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.functions.AggregateFunction



 //org.apache.flink.streaming.api.functions.windowing.AllWindowFunction

class DataStreamProcTimeAggregateGlobalWindowFunction[W <: Window](
     private val aggregates: Array[AggregateFunction[_]],
     private val aggFields: Array[Int],
     private val forwardedFieldCount: Int)
     extends RichAllWindowFunction[Row, Row, W] {
  
private var output: Row = _
private var accumulators: Row= _
 

 override def open(parameters: Configuration): Unit = {
     output = new Row(forwardedFieldCount + aggregates.length)
     accumulators = new Row(aggregates.length)
     var i = 0
     while (i < aggregates.length) {
        accumulators.setField(i, aggregates(i).createAccumulator())
        i = i + 1
     }
  }
   
 override def apply(
      window: W,
      records: Iterable[Row],
      out: Collector[Row]): Unit = {

  
     var i = 0
     //initialize the values of the aggregators by re-creating them
     //the design of the Accumulator interface should be extended to enable 
     //a reset function for better performance
     while (i < aggregates.length) {
        accumulators.setField(i, aggregates(i).createAccumulator())
        i += 1
     }
     var reuse:Row = null
     //iterate through the elements and aggregate
     val iter = records.iterator
     while (iter.hasNext) {
       reuse = iter.next
       i = 0
       while (i < aggregates.length) {
          val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
          aggregates(i).accumulate(accumulator, reuse.getField(aggFields(i)))
          i += 1
       }
     }

    //set the values of the result with current elements values if needed
    i = 0
    while (i < forwardedFieldCount) {
      output.setField(i, reuse.getField(i))
      i += 1
    }
    
    //set the values of the result with the accumulators
    i = 0
    while (i < aggregates.length) {
      val index = forwardedFieldCount + i
      val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
      output.setField(index, aggregates(i).getValue(accumulator))
      i += 1
    }

    out.collect(output)

  
    }
  
}
