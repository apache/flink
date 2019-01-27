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

package org.apache.flink.table.expressions

import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.SqlFunction
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.functions.sql.ScalarSqlFunctions
import org.apache.flink.table.plan.logical.LogicalExprVisitor
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

abstract class HashExpression(args: Expression*) extends Expression {
  private[flink] def hashName: String
  private[flink] def hashFunction: SqlFunction

  override private[flink] def children = args
  override private[flink] def resultType: InternalType = DataTypes.STRING

  override def toString: String = s"$hashName(${args.mkString(", ")})"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(hashFunction, args.map(_.toRexNode): _*)
  }

  override private[flink] def validateInput(): ValidationResult = {
    if (args.length == 1) {
      args(0).resultType match {
        case DataTypes.CHAR | DataTypes.STRING => ValidationSuccess
        case _ => ValidationFailure(s"Argument of $hashName function must be string or character.")
      }
    } else if (args.length == 2) {
      (args(0).resultType, args(1).resultType) match {
        case (DataTypes.CHAR | DataTypes.STRING, DataTypes.CHAR | DataTypes.STRING) =>
          ValidationSuccess
        case _ => ValidationFailure(s"Argument of $hashName function must be string or character.")
      }
    } else {
      ValidationFailure(s"$hashName function must have 1 or 2 arguments.")
    }
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Md5(args: Expression*) extends HashExpression(args: _*) {
  override private[flink] def hashName = "md5"
  override private[flink] def hashFunction = ScalarSqlFunctions.MD5
}

case class Sha1(args: Expression*) extends HashExpression(args: _*) {
  override private[flink] def hashName = "sha1"
  override private[flink] def hashFunction = ScalarSqlFunctions.SHA1
}

case class Sha224(args: Expression*) extends HashExpression(args: _*) {
  override private[flink] def hashName = "sha224"
  override private[flink] def hashFunction = ScalarSqlFunctions.SHA224
}

case class Sha256(args: Expression*) extends HashExpression(args: _*) {
  override private[flink] def hashName = "sha256"
  override private[flink] def hashFunction = ScalarSqlFunctions.SHA256
}

case class Sha384(args: Expression*) extends HashExpression(args: _*) {
  override private[flink] def hashName = "sha384"
  override private[flink] def hashFunction = ScalarSqlFunctions.SHA384
}

case class Sha512(args: Expression*) extends HashExpression(args: _*) {
  override private[flink] def hashName = "sha512"
  override private[flink] def hashFunction = ScalarSqlFunctions.SHA512
}

case class Sha2(args: Expression*) extends HashExpression(args: _*) {
  override private[flink] def hashName = "sha2"
  override private[flink] def hashFunction = ScalarSqlFunctions.SHA2

  override private[flink] def validateInput(): ValidationResult = {
    if (args.length == 2) {
      (args(0).resultType, args(1).resultType) match {
        case (DataTypes.CHAR | DataTypes.STRING, DataTypes.INT) =>
          ValidationSuccess
        case _ => ValidationFailure(s"Argument of $hashName function must be (string, int)")
      }
    } else if (args.length == 3) {
      (args(0).resultType, args(1).resultType, args(2).resultType) match {
        case (DataTypes.CHAR | DataTypes.STRING,
        DataTypes.CHAR | DataTypes.STRING, DataTypes.INT) =>
          ValidationSuccess
        case _ => ValidationFailure(s"Argument of $hashName function must be (string, string, int)")
      }
    } else {
      ValidationFailure(s"$hashName function must have 2 or 3 arguments.")
    }
  }
}
