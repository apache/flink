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

import scala.language.experimental.macros

/**
 * ==Table & SQL API==
 *
 * This package contains the API of the Table & SQL API. Users create [[Table]]s on which relational
 * operations can be performed.
 *
 * For accessing all API classes and implicit conversions, use the following imports:
 *
 * {{{
 *   import org.apache.flink.table.api._
 * }}}
 *
 * More information about the entry points of the API can be found in [[TableEnvironment]].
 *
 * Available implicit expressions are listed in [[ImplicitExpressionConversions]] and
 * [[ImplicitExpressionOperations]].
 *
 * Please refer to the website documentation about how to construct and run table programs.
 *
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
package object api extends ImplicitExpressionConversions with ImplicitTypeConversions {

  val FLIP_265_WARNING: String = "All Flink Scala APIs are deprecated and will be removed in a " +
    "future Flink version. You can still build your application in Scala, but you should move " +
    "to the Java version of either the DataStream and/or Table API."

}
