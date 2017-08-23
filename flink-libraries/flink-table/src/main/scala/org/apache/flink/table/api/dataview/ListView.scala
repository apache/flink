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

package org.apache.flink.table.api.dataview

import java.lang.{Iterable => JIterable}
import java.util

import org.apache.flink.api.common.typeinfo.{TypeInfo, TypeInformation}
import org.apache.flink.table.dataview.ListViewTypeInfoFactory

/**
  * ListView provides List functionality for accumulators used by user-defined aggregate functions
  * {{AggregateFunction}}.
  *
  * A ListView can be backed by a Java ArrayList or a state backend, depending on the context in
  * which the function is used.
  *
  * At runtime `ListView` will be replaced by a [[org.apache.flink.table.dataview.StateListView]]
  * when use state backend..
  *
  * Example:
  * {{{
  *
  *  public class MyAccum {
  *    public ListView<String> list;
  *    public long count;
  *  }
  *
  *  public class MyAgg extends AggregateFunction<Long, MyAccum> {
  *
  *   @Override
  *   public MyAccum createAccumulator() {
  *     MyAccum accum = new MyAccum();
  *     accum.list = new ListView<>(Types.STRING);
  *     accum.count = 0L;
  *     return accum;
  *   }
  *
  *   public void accumulate(MyAccum accumulator, String id) {
  *     accumulator.list.add(id);
  *     ... ...
  *     accumulator.get()
  *     ... ...
  *   }
  *
  *   @Override
  *   public Long getValue(MyAccum accumulator) {
  *     accumulator.list.add(id);
  *     ... ...
  *     accumulator.get()
  *     ... ...
  *     return accumulator.count;
  *   }
  * }
  *
  * }}}
  *
  * @param elementTypeInfo element type information
  * @tparam T element type
  */
@TypeInfo(classOf[ListViewTypeInfoFactory[_]])
class ListView[T](
    @transient private[flink] val elementTypeInfo: TypeInformation[T])
  extends DataView {

  def this() = this(null)

  private[flink] val list = new util.ArrayList[T]()

  /**
    * Returns an iterable of the list.
    *
    * @throws Exception Thrown if the system cannot get data.
    * @return The iterable of the list or { @code null} if the list is empty.
    */
  @throws[Exception]
  def get: JIterable[T] = {
    if (!list.isEmpty) {
      list
    } else {
      null
    }
  }

  /**
    * Adding the given value to the list.
    *
    * @throws Exception Thrown if the system cannot add data.
    * @param value element to be appended to this list
    */
  @throws[Exception]
  def add(value: T): Unit = list.add(value)

  /**
    * Copies all of the elements from the specified list to this list view.
    *
    * @throws Exception Thrown if the system cannot add all data.
    * @param list The list to be stored in this list view.
    */
  @throws[Exception]
  def addAll(list: util.List[T]): Unit = this.list.addAll(list)

  /**
    * Removes all of the elements from this list.
    *
    * The list will be empty after this call returns.
    */
  override def clear(): Unit = list.clear()

  override def equals(other: Any): Boolean = other match {
    case that: ListView[_] =>
      list.equals(that.list)
    case _ => false
  }

  override def hashCode(): Int = list.hashCode()
}
