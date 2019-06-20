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

package org.apache.flink.table.plan.util

import org.apache.flink.table.util.Logging

import org.apache.calcite.rex._

import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.collection.mutable

object RexNodeExtractor extends Logging {

  /**
    * Extracts the indices of input fields which accessed by the expressions.
    *
    * @param exprs The RexNode list to analyze
    * @return The indices of accessed input fields
    */
  def extractRefInputFields(exprs: JList[RexNode]): Array[Int] = {
    val visitor = new InputRefVisitor
    // extract referenced input fields from expressions
    exprs.foreach(_.accept(visitor))
    visitor.getFields
  }

  /**
    * Extracts the name of nested input fields accessed by the expressions and returns the
    * prefix of the accesses.
    *
    * @param exprs The expressions to analyze
    * @param usedFields indices of used input fields
    * @return The full names of accessed input fields. e.g. field.subfield
    */
  def extractRefNestedInputFields(
      exprs: JList[RexNode],
      usedFields: Array[Int]): Array[Array[String]] = {
    val visitor = new RefFieldAccessorVisitor(usedFields)
    exprs.foreach(_.accept(visitor))
    visitor.getProjectedFields
  }
}

/**
  * An RexVisitor to extract all referenced input fields
  */
class InputRefVisitor extends RexVisitorImpl[Unit](true) {

  private val fields = mutable.LinkedHashSet[Int]()

  def getFields: Array[Int] = fields.toArray

  override def visitInputRef(inputRef: RexInputRef): Unit =
    fields += inputRef.getIndex

  override def visitCall(call: RexCall): Unit =
    call.operands.foreach(operand => operand.accept(this))
}

/**
  * A RexVisitor to extract used nested input fields
  */
class RefFieldAccessorVisitor(usedFields: Array[Int]) extends RexVisitorImpl[Unit](true) {

  private val projectedFields: Array[Array[String]] = Array.fill(usedFields.length)(Array.empty)

  private val order: Map[Int, Int] = usedFields.zipWithIndex.toMap

  /** Returns the prefix of the nested field accesses */
  def getProjectedFields: Array[Array[String]] = {

    projectedFields.map { nestedFields =>
      // sort nested field accesses
      val sorted = nestedFields.sorted
      // get prefix field accesses
      val prefixAccesses = sorted.foldLeft(Nil: List[String]) {
        (prefixAccesses, nestedAccess) =>
          prefixAccesses match {
            // first access => add access
            case Nil => List[String](nestedAccess)
            // top-level access already found => return top-level access
            case head :: Nil if head.equals("*") => prefixAccesses
            // access is top-level access => return top-level access
            case _ :: _ if nestedAccess.equals("*") => List("*")
            // previous access is not prefix of this access => add access
            case head :: _ if !nestedAccess.startsWith(head) =>
              nestedAccess :: prefixAccesses
            // previous access is a prefix of this access => do not add access
            case _ => prefixAccesses
          }
      }
      prefixAccesses.toArray
    }
  }

  override def visitFieldAccess(fieldAccess: RexFieldAccess): Unit = {
    def internalVisit(fieldAccess: RexFieldAccess): (Int, String) = {
      fieldAccess.getReferenceExpr match {
        case ref: RexInputRef =>
          (ref.getIndex, fieldAccess.getField.getName)
        case fac: RexFieldAccess =>
          val (i, n) = internalVisit(fac)
          (i, s"$n.${fieldAccess.getField.getName}")
      }
    }

    val (index, fullName) = internalVisit(fieldAccess)
    val outputIndex = order.getOrElse(index, -1)
    val fields: Array[String] = projectedFields(outputIndex)
    projectedFields(outputIndex) = fields :+ fullName
  }

  override def visitInputRef(inputRef: RexInputRef): Unit = {
    val outputIndex = order.getOrElse(inputRef.getIndex, -1)
    val fields: Array[String] = projectedFields(outputIndex)
    projectedFields(outputIndex) = fields :+ "*"
  }

  override def visitCall(call: RexCall): Unit =
    call.operands.foreach(operand => operand.accept(this))
}
