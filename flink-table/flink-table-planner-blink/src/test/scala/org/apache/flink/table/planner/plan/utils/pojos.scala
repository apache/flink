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
package org.apache.flink.table.planner.plan.utils

class MyPojo() {
  var f1: Int = 0
  var f2: String = ""

  def this(f1: Int, f2: String) {
    this()
    this.f1 = f1
    this.f2 = f2
  }

  override def equals(other: Any): Boolean = other match {
    case that: MyPojo =>
      (that canEqual this) &&
        f1 == that.f1 &&
        f2 == that.f2
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[MyPojo]

  override def toString = s"MyPojo($f1, $f2)"
}

class NonPojo {
  val x = new java.util.HashMap[String, String]()

  override def toString: String = x.toString

  override def hashCode(): Int = super.hashCode()

  override def equals(obj: scala.Any): Boolean = super.equals(obj)
}
