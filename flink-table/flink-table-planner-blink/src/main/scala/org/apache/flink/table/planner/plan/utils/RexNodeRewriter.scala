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

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._

import java.util.{Collections, Arrays, List => JList, Map => JMap}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object RexNodeRewriter {

  /**
    * Generates new expressions with used input fields.
    *
    * @param usedFields indices of used input fields
    * @param exps       original expression lists
    * @return new expression with only used input fields
    */
  def rewriteWithNewFieldInput(
      exps: JList[RexNode],
      usedFields: Array[Int]): JList[RexNode] = {
    // rewrite input field in expressions
    val inputRewriter = new InputRewriter(usedFields.zipWithIndex.toMap)
    exps.map(_.accept(inputRewriter)).toList.asJava
  }

  /**
   * Generates new expressions with used input fields.
   *
   * @param exps       original expression lists
   * @param fieldMap   mapping between old index of the new index
   * @param rowTypes   type of the used fields
   * @param rexBuilder builder to build field access
   * @return new expression with only used input fields
   */
  def rewriteNestedProjectionWithNewFieldInput(
      exps: JList[RexNode],
      fieldMap: JMap[Integer, JMap[JList[String], Integer]],
      rowTypes: JList[RelDataType],
      rexBuilder: RexBuilder): JList[RexNode] = {
    val writer = new NestedInputRewriter(fieldMap, rowTypes, rexBuilder)
    exps.map(_.accept(writer)).toList.asJava
  }
}

/**
  * A RexShuttle to rewrite field accesses of RexNode.
  *
  * @param fieldMap old input fields ref index -> new input fields ref index mappings
  */
class InputRewriter(fieldMap: Map[Int, Int]) extends RexShuttle {

  override def visitInputRef(inputRef: RexInputRef): RexNode =
    new RexInputRef(refNewIndex(inputRef), inputRef.getType)

  override def visitLocalRef(localRef: RexLocalRef): RexNode =
    new RexInputRef(refNewIndex(localRef), localRef.getType)

  private def refNewIndex(ref: RexSlot): Int =
    fieldMap.getOrElse(ref.getIndex,
      throw new IllegalArgumentException("input field contains invalid index"))
}

/**
 * A RexShuttle to rewrite field accesses of RexNode with nested projection.
 * For `RexInputRef`, it works like `InputReWriter` and use the old input
 * ref index to find the new input fields ref.
 * For `RexFieldAccess`, it will traverse to the top level of the access and
 * find the mapping in field fieldMap first. There are 3 situations we need to consider:
 *  1. mapping has the top level access, we should make field access to the reference;
 *  2. mapping has the field, we should make an access;
 *  3. mapping has no information of the current name, we should keep the full name
 *  of the fields and index of mapping for later lookup.
 * When the process is back from the recursion, we still have 2 situations need to
 * consider:
 *  1. we have found the reference of the upper level, we just make an access above the
 *  reference we find before;
 *  2. we haven't found the reference of the upper level, we concatenate the prefix with
 *  the current field name and look up the new prefix in the mapping. If it's in the mapping,
 *  we create a reference. Otherwise, we should go to the next level with the new prefix.
 */
class NestedInputRewriter(
  fieldMap: JMap[Integer, JMap[JList[String], Integer]],
  rowTypes: JList[RelDataType],
  builder: RexBuilder) extends RexShuttle {

  val topLevelIdentifier = Collections.singletonList("*")

  override def visitFieldAccess(input: RexFieldAccess): RexNode = {
    val (_, _, node) = traverse(input)
    if (node.isDefined) {
      node.get
    } else {
      throw new IllegalArgumentException("input field access is not find in the field mapping.")
    }
  }

  override def visitInputRef(inputRef: RexInputRef): RexNode = {
    val mapping = fieldMap.getOrElse(inputRef.getIndex,
      throw new IllegalArgumentException("input field contains unknown index"))
    if (mapping.contains(topLevelIdentifier)) {
      new RexInputRef(mapping(topLevelIdentifier), rowTypes(mapping(topLevelIdentifier)))
    } else {
      throw new IllegalArgumentException("empty field mapping")
    }
  }

  private def traverse(fieldAccess: RexFieldAccess): (Int, JList[String], Option[RexNode]) = {
    fieldAccess.getReferenceExpr match {
      case ref: RexInputRef =>
        val mapping =
          fieldMap.getOrElse(ref.getIndex,
            throw new IllegalArgumentException("input field contains unknown index"))
        val key = Arrays.asList(fieldAccess.getField.getName)
        if (mapping.contains(topLevelIdentifier)) {
          (ref.getIndex,
            Collections.emptyList(),
            Some(builder.makeFieldAccess(
              new RexInputRef(mapping(topLevelIdentifier), rowTypes(mapping(topLevelIdentifier))),
              fieldAccess.getField.getName,
              false))
          )
        } else if(mapping.contains(key)) {
          (ref.getIndex,
            Collections.emptyList(),
            Some(new RexInputRef(mapping(key), rowTypes(mapping(key)))))
        } else {
          (ref.getIndex, Arrays.asList(fieldAccess.getField.getName), Option.empty)
        }
      case acc: RexFieldAccess =>
        val (i, prefix, node) = traverse(acc)
        if (node.isDefined) {
          (i,
            Collections.emptyList(),
            Some(builder.makeFieldAccess(node.get, fieldAccess.getField.getName, false)))
        } else {
          val newPrefix = prefix :+ fieldAccess.getField.getName
          // we have checked before
          val mapping = fieldMap(i)
          if (mapping.contains(newPrefix)) {
            (i,
              Collections.emptyList(),
              Some(new RexInputRef(mapping(newPrefix), rowTypes(mapping(newPrefix)))))
          } else {
            (i, newPrefix, Option.empty)
          }
        }
    }
  }
}
