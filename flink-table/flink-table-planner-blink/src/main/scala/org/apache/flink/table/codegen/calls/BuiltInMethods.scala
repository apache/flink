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

package org.apache.flink.table.codegen.calls

import org.apache.calcite.linq4j.tree.Types
import org.apache.flink.table.runtime.functions.DateTimeUtils

import java.util.TimeZone

object BuiltInMethods {

  val STRING_TO_TIMESTAMP = Types.lookupMethod(
    classOf[DateTimeUtils],
    "strToTimestamp",
    classOf[String], classOf[TimeZone])

  val UNIX_TIME_TO_STRING = Types.lookupMethod(
    classOf[DateTimeUtils],
    "timeToString",
    classOf[Int])

  val TIMESTAMP_TO_STRING = Types.lookupMethod(
    classOf[DateTimeUtils],
    "timestampToString",
    classOf[Long], classOf[Int], classOf[TimeZone])
}
