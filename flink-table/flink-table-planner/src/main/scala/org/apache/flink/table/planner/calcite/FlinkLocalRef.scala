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
package org.apache.flink.table.planner.calcite

import org.apache.flink.table.planner.codegen.ExprCodeGenerator
import org.apache.flink.table.types.logical.LogicalType

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexBiVisitor, RexVariable, RexVisitor}

/**
  * Special reference which represent a local field, such as aggregate buffers or constants.
  * We are stored as class members, so the field can be referenced directly.
  * We should use an unique name to locate the field.
  */
case class RexFieldVariable(
    fieldTerm: String,
    dataType: RelDataType) extends RexVariable(fieldTerm, dataType) {
  
  override def accept[R](visitor: RexVisitor[R]): R = {
    visitor match {
      case gen: ExprCodeGenerator =>
        gen.visitRexFieldVariable(this).asInstanceOf[R]
      case _ =>
        throw new RuntimeException("Not support visitor: " + visitor)
    }
  }
  
  override def accept[R, P](visitor: RexBiVisitor[R, P], arg: P): R = {
    throw new RuntimeException("Not support visitor: " + visitor)
  }
}

/**
  * Special reference which represent a distinct key input filed,
  * We use the name to locate the distinct key field.
  */
case class RexDistinctKeyVariable(
    keyTerm: String,
    dataType: RelDataType,
    internalType: LogicalType) extends RexVariable(keyTerm, dataType) {

  override def accept[R](visitor: RexVisitor[R]): R = {
    visitor match {
      case gen: ExprCodeGenerator =>
        gen.visitDistinctKeyVariable(this).asInstanceOf[R]
      case _ =>
        throw new RuntimeException("Not support visitor: " + visitor)
    }
  }

  override def accept[R, P](visitor: RexBiVisitor[R, P], arg: P): R = {
    throw new RuntimeException("Not support visitor: " + visitor)
  }
}
