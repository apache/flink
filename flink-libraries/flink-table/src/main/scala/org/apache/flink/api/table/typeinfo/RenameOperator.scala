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
package org.apache.flink.api.table.typeinfo

import org.apache.flink.api.common.operators.Operator
import org.apache.flink.api.java.operators.SingleInputOperator
import org.apache.flink.api.java.{DataSet => JavaDataSet}

/**
 * This is a logical operator that can hold a [[RenamingProxyTypeInfo]] for renaming some
 * fields of a [[org.apache.flink.api.common.typeutils.CompositeType]]. At runtime this
 * disappears since the translation methods simply returns the input.
 */
class RenameOperator[T](
    input: JavaDataSet[T],
    renamingTypeInformation: RenamingProxyTypeInfo[T])
  extends SingleInputOperator[T, T, RenameOperator[T]](input, renamingTypeInformation) {

  override protected def translateToDataFlow(
      input: Operator[T]): Operator[T] = input
}
