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
package org.apache.flink.table

package object planner {

  type JBoolean = java.lang.Boolean
  type JByte = java.lang.Byte
  type JShort = java.lang.Short
  type JFloat = java.lang.Float
  type JInt = java.lang.Integer
  type JLong = java.lang.Long
  type JDouble = java.lang.Double
  type JString = java.lang.String
  type JBigDecimal = java.math.BigDecimal

  type JList[E] = java.util.List[E]
  type JArrayList[E] = java.util.ArrayList[E]

  type JMap[K, V] = java.util.Map[K, V]
  type JHashMap[K, V] = java.util.HashMap[K, V]

  type JSet[E] = java.util.Set[E]
  type JHashSet[E] = java.util.HashSet[E]

  type CalcitePair[T, R] = org.apache.calcite.util.Pair[T, R]

}
