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

import org.apache.flink.api.common.typeinfo.{TypeInfo, TypeInformation}
import org.apache.flink.table.dataview.ListViewTypeInfoFactory

/**
  * ListView encapsulates the operation of list.
  *
  * All methods in this class are not implemented, users do not need to care about whether it is
  * backed by Java ArrayList or state backend. It will be replaced by a {@link StateListView} or a
  * {@link HeapListView}.
  *
  * <p>
  *     <b>NOTE:</b> Users are not recommended to extends this class.
  * </p>
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
  *   //Overloaded accumulate method
  *   public void accumulate(MyAccum accumulator, String id) {
  *     accumulator.list.add(id);
  *     ... ...
  *   }
  *
  *   @Override
  *   public Long getValue(MyAccum accumulator) {
  *     ... ...
  *     // accumulator.get()
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
class ListView[T](val elementTypeInfo: TypeInformation[T]) extends DataView {

  def this() = this(null)

  /**
    * Returns an iterable of the list.
    *
    * @return The iterable of the list or { @code null} if the list is empty.
    */
  def get: JIterable[T] = throw new UnsupportedOperationException("Unsupported operation!")

  /**
    * Adding the given value to the list.
    *
    * @param value element to be appended to this list
    */
  def add(value: T): Unit = throw new UnsupportedOperationException("Unsupported operation!")

  /**
    * Removes all of the elements from this list.
    *
    * The list will be empty after this call returns.
    */
  override def clear(): Unit = throw new UnsupportedOperationException("Unsupported operation!")
}
