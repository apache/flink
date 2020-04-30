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
package org.apache.flink.streaming.api.scala

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.{OutputTag => JOutputTag}


/**
 * An [[OutputTag]] is a typed and named tag to use for tagging side outputs
 * of an operator.
 *
 * Example:
 * {{{
 *   val outputTag = OutputTag[String]("late-data")
 * }}}
 *
 * @tparam T the type of elements in the side-output stream.
 */
@PublicEvolving
class OutputTag[T: TypeInformation](
    id: String) extends JOutputTag[T](id, implicitly[TypeInformation[T]])

object OutputTag {
  def apply[T: TypeInformation](id: String): OutputTag[T] = new OutputTag(id)
}
