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
package org.apache.flink.api.expressions.operations

import org.apache.flink.api.common.typeinfo.TypeInformation

/**
 * When an [[org.apache.flink.api.expressions.ExpressionOperation]] is created an
 * [[OperationTranslator]] corresponding to the underlying representation (either
 * [[org.apache.flink.api.scala.DataSet]] or [[org.apache.flink.streaming.api.scala.DataStream]] is
 * created. This way, the expression API can be completely agnostic while translation back to the
 * correct API is handled by the API specific translator.
 */
abstract class OperationTranslator {

  type Representation[A]

  def translate[A](op: Operation)(implicit tpe: TypeInformation[A]): Representation[A]

}
