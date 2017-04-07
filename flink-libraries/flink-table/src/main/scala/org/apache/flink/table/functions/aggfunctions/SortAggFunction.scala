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
package org.apache.flink.table.functions.aggfunctions

import java.math.BigDecimal
import java.util.{List => JList}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import scala.collection.mutable.ArrayBuffer
import org.apache.flink.types.Row
import org.apache.flink.table.runtime.aggregate.UntypedOrdering
import org.apache.flink.table.runtime.aggregate.MultiOutputAggregateFunction
import java.util.{ List => JList,ArrayList }
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.calcite.rel.RelFieldCollation.Direction

/** The initial accumulator for Sort aggregate function */
class SortAccumulator extends ArrayBuffer[JTuple2[Row,Row]] with Accumulator with Serializable

/**
  * Base class for built-in Min aggregate function
  *
  * @tparam K the type for the key sort type
  * @tparam T the type for the aggregation result
  */
abstract class SortAggFunction[K,T](
    val keyIndexes: Array[Int],
    val keySortDirections: Array[Direction],
    val orderings: Array[UntypedOrdering]) extends MultiOutputAggregateFunction[T] {

  override def createAccumulator(): Accumulator = {
    val acc = new SortAccumulator
      
    acc
  }

  override def accumulate(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[Row]
      val acc = accumulator.asInstanceOf[SortAccumulator]
      
      var i = 0
      //create the (compose) key of the new value
      val keyV = new Row(keyIndexes.size)
      while (i<keyIndexes.size) {
        keyV.setField(i, v.getField(keyIndexes(i)))
        i += 1
      }
        
      var j = 0
      while (j<acc.size) {
        i = 0
        while (i<keyIndexes.size) {
          
          val compareResult = if(keySortDirections(i) == Direction.ASCENDING) {
            orderings(i).compare(acc(j).f0.getField(i),keyV.getField(i))
          } else {
            orderings(i).compare(keyV.getField(i),acc(j).f0.getField(i))
          }
          
          compareResult match {
          case 0 => i += 1 //same key and need to sort on consequent keys 
          case g if g > 0 => {
            acc.insert(j, new JTuple2(keyV,v)) //add new element in place
            //end loop and ensure that the element is not added again
            j = acc.size + 1 
            i = keyIndexes.size
          }
          case _ => i = keyIndexes.size // continue with the next element
          }
        }
       j += 1
      }
      
      //condition for the case when the element needs to be inserted at the end as greatest element
      if (j==acc.size) {
        acc.insert(j, new JTuple2(keyV,v))
      }
      
    }
  }

  override def getValue(accumulator: Accumulator): T = {
       null.asInstanceOf[T]    
  }
  
  override def getValues(accumulator: Accumulator): JList[T] = {
    val acc = accumulator.asInstanceOf[SortAccumulator]
    val res = new ArrayList[T](acc.size)
    
    var i=0
    while (i< acc.size) {
      res.add(acc(i).f1.asInstanceOf[T])
      i += 1
    }
    
    res    
  }

  override def merge(accumulators: JList[Accumulator]): Accumulator = {
    val ret = accumulators.get(0)
    var i: Int = 1
    var j = 0
    while (i < accumulators.size()) {
      val a = accumulators.get(i).asInstanceOf[SortAccumulator]
      j = 0
      while (j < a.size) {
        accumulate(ret.asInstanceOf[SortAccumulator], a(j).f1)
        j += 1
      }
      i += 1
    }
    ret
  }

  override def resetAccumulator(accumulator: Accumulator): Unit = {
    accumulator.asInstanceOf[SortAccumulator].clear()
  }

   override def getAccumulatorType(): TypeInformation[_] = {
          
     new ListTypeInfo(
         new TupleTypeInfo(new JTuple2[Row,Row].getClass,
             getValueTypeInfo,
             getSortKeyTypeInfo)
     )
   
  }


  def getValueTypeInfo: TypeInformation[_]
  def getSortKeyTypeInfo: TypeInformation[_]
}

/**
  * Built-in Row sort aggregate function
  */
class RowSortAggFunction(keyIndexes: Array[Int],
    keySortDirections: Array[Direction],
    orderings: Array[UntypedOrdering],
    rowType: RowTypeInfo,
    sortKeyType:RowTypeInfo) 
    extends SortAggFunction[Row,Row](keyIndexes,keySortDirections,orderings) {
  override def getValueTypeInfo = rowType
  override def getSortKeyTypeInfo = sortKeyType
}

