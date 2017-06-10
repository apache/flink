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
package org.apache.flink.table.functions

import org.apache.commons.codec.digest.DigestUtils
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.serialize
/**
  * Base class for all user-defined functions such as scalar functions, table functions,
  * or aggregation functions.
  */
abstract class UserDefinedFunction extends Serializable {
  /**
    * Setup method for user-defined function. It can be used for initialization work.
    *
    * By default, this method does nothing.
    */
  @throws(classOf[Exception])
  def open(context: FunctionContext): Unit = {}

  /**
    * Tear-down method for user-defined function. It can be used for clean up work.
    *
    * By default, this method does nothing.
    */
  @throws(classOf[Exception])
  def close(): Unit = {}

  final def functionIdentifier: String = {
    val md5  =  DigestUtils.md5Hex(serialize(this))
    getClass.getCanonicalName.replace('.', '$').concat("$").concat(md5)
  }
}
