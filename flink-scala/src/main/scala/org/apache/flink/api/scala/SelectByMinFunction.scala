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
package org.apache.flink.api.scala

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase

/**
  * SelectByMinFunction to work with Scala tuples
  */
@Internal
class SelectByMinFunction[T](t : TupleTypeInfoBase[T], fields : Array[Int])
  extends ReduceFunction[T] {
  for(f <- fields) {
    if (f < 0 || f >= t.getArity()) {
      throw new IndexOutOfBoundsException(
        "SelectByMinFunction field position " + f + " is out of range.")
    }

    // Check whether type is comparable
    if (!t.getTypeAt(f).isKeyType()) {
      throw new IllegalArgumentException(
        "SelectByMinFunction supports only key(Comparable) types.")
    }
  }

  override def reduce(value1: T, value2: T): T = {
    for (f <- fields) {
        val element1  = value1.asInstanceOf[Product].productElement(f).asInstanceOf[Comparable[Any]]
        val element2 = value2.asInstanceOf[Product].productElement(f).asInstanceOf[Comparable[Any]]

        val comp = element1.compareTo(element2)

        // If comp is bigger than 0 comparable 1 is bigger.
        // Return the smaller value.
        if (comp < 0) {
          return value1
        } else if (comp > 0) {
          return value2
        }
    }
    value1
  }
}
