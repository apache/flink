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

import org.apache.flink.table.api.TableException

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._

import java.util
import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * RexNodeNestedField is a tree node to build the used fields tree.
 *
 * @param name                 The name of the fields in the origin schema
 * @param indexInOriginSchema  The index of the field in the origin schema.
 *                             It only works for the RowType.
 * @param fieldType            The type of the field. It is useful when
 *                             rewriting the projections.
 * @param useAll               Mark the field is the leaf node in the tree.
 * @param children             Store the children of the field. It's safe
 *                             to use name as the index because name is
 *                             unique in every level. It also uses
 *                             the LinkedHashMap to keep the insert order.
 *                             In some cases, it can reduce the cost of the
 *                             reorder of the fields in query.
 * @param order                For leaf node, the order is used to memorize
 *                             the location of the field in the new schema.
 *                             For root and  node, it is used to memorize the
 *                             number of leaf node.
 *                             For intermediate node, it's useless.
 */
class RexNodeNestedField(
    val name: String,
    val indexInOriginSchema: Int,
    val fieldType: RelDataType,
    var useAll: Boolean,
    val children: util.LinkedHashMap[String, RexNodeNestedField],
    var order: Int) {

  def addChild(field: RexNodeNestedField): Unit = {
    if (!children.contains(field.name)) {
      useAll = false
      children.put(field.name, field)
    }
  }

  def deleteChild(fieldName: String): Option[RexNodeNestedField] = {
    if (children.containsKey(fieldName)) {
      Some(children.remove(fieldName))
    } else {
      Option.empty
    }
  }
}

object RexNodeNestedField {
  /**
   * It will uses the RexNodes to build a tree of the used fields.
   * It uses a visitor to visit the operands of the expression. For
   * input ref, it sits on the top level of the schema and it is the
   * direct child of the root. For field access, it first decompose
   * the field into a list and then create the node for every node in
   * the list.
   *
   * In some situation, it will delete node. For example, the input
   * expressions are "$0.child" and "$0". It will first create the
   * intermediate node "$0" and leaf node "child". When coming to the
   * expression "$0", it indicate the query will use the whole fields "$0"
   * rather than the child "child" only. In this situation, it will mark
   * the node "$0" as a leaf node and delete its children.
   * */
  def build(exprs: JList[RexNode], rowType: RelDataType):
      RexNodeNestedField = {
    // the order field in the root node is to memorize
    // the number of leaf
    val root = new RexNodeNestedField(
      "root",
      0,
      rowType,
      false,
      new util.LinkedHashMap[String, RexNodeNestedField](),
      -1)
    val visitor = new NestedFieldExtractor(root, rowType)
    for(expr <- exprs) {
      expr.accept(visitor)
    }
    root
  }

  /**
   * After the projection, the used fields location has been changed.
   * If the node in the tree has been labeled with the order, it will
   * rewrite the location in the old schema with the new location.
   *
   * It uses a visitor to visit operands of the RexNode. If the type of
   * operand is InputRef, it still in the top level of the schema and get
   * the location of the fields using map. If the type of the operand is
   * FieldAccess, it will first traverse to the top level of the field and
   * iterate every level of the field with the name in the RexNode. For more
   * details, please refer to NestedFieldReWriter.
   */
  def rewrite(
      exprs: JList[RexNode],
      root: RexNodeNestedField,
      builder: RexBuilder): JList[RexNode] = {
    val writer = new NestedFieldReWriter(root, builder)
    exprs.map(_.accept(writer)).toList.asJava
  }

  /**
   * It will label the order of the leaf node with the insert order rather
   * than the natural order of the name and output the path to the every
   * leaf node. The paths are useful for interface SupportsProjectionPushDown
   * and test(debug).
   */
  def labelAndConvert(root: RexNodeNestedField): Array[Array[Int]] = {
    val allPaths = new util.LinkedList[Array[Int]]()
    traverse(root, 0, new util.LinkedList[Int](), allPaths)
    allPaths.toArray(new Array[Array[Int]](0))
  }

  private def traverse(
      parent: RexNodeNestedField,
      order: Int,
      path: JList[Int],
      allPaths: JList[Array[Int]]): Int ={
    val tail = path.size()
    // push self
    path.add(parent.indexInOriginSchema)
    val newOrder = if (parent.useAll) {
      // leaf node
      parent.order = order
      // ignore root node
      allPaths.add(path.slice(1, tail + 1).toArray)
      order + 1
    } else {
      // iterate children
      parent.children.values().foldLeft(order) {
        case (newOrder, child) =>
          traverse(child, newOrder, path, allPaths)
      }
    }
    // pop self
    path.remove(tail)
    newOrder
  }
}

/**
 * A RexShuttle to rewrite field accesses of RexNode with nested projection.
 * For `RexInputRef`, it uses the old input ref name to find the new input fields ref
 * and use the order to generate the new input ref.
 * For `RexFieldAccess`, it will traverse to the top level of the field access and
 * then to generate new RexNode. There are 3 situations we need to consider:
 *  1. if top level field is marked to use all sub-fields , make field access of the reference
 *  and warp the ref as RexFieldAccess with the sub field name;
 *  2. if top level field isn't marked to use all sub-fields and its direct field
 *  is marked as useall, make field reference of the direct subfield;
 *  3. if neither situation above happens, return from the recursion with the updated parent.
 * When the process is back from the recursion, it still has 2 situations need to
 * consider:
 *  1. if the process has found the reference of the upper level, just make an access on the
 *  reference founded before;
 *  2. if the process hasn't found the first reference, the process continues to search under
 *  the current parent.
 */
private class NestedFieldReWriter(
    root: RexNodeNestedField,
    builder: RexBuilder) extends RexShuttle {
  override def visitInputRef(inputRef: RexInputRef): RexNode = {
    if (!root.children.containsKey(inputRef.getName)) {
      throw new TableException(
        "Illegal input field access" + inputRef.getName)
    } else {
      val field = root.children.get(inputRef.getName)
      new RexInputRef(field.order, field.fieldType)
    }
  }

  override def visitFieldAccess(fieldAccess: RexFieldAccess): RexNode = {
    val (node, _) = traverse(fieldAccess)
    if (node.isDefined) {
      node.get
    } else {
      throw new TableException(
        "Unknown field " + fieldAccess + " when rewrite projection ")
    }
  }

  private def traverse(
      fieldAccess: RexFieldAccess): (Option[RexNode], RexNodeNestedField) = {
    fieldAccess.getReferenceExpr match {
      case ref: RexInputRef =>
        val parent = root.children.get(ref.getName)
        if (parent.useAll) {
          (
            Some(builder.makeFieldAccess(
              new RexInputRef(parent.order, parent.fieldType),
              fieldAccess.getField.getName,
              true)),
            root)
        } else {
          val child = parent.children.get(fieldAccess.getField.getName)
          if (child.useAll) {
            (Some(new RexInputRef(child.order, child.fieldType)), root)
          } else {
            (Option.empty, child)
          }
        }
      case acc: RexFieldAccess =>
        val (field, parent) = traverse(acc)
        if (field.isDefined) {
          (
            Some(
              builder.makeFieldAccess(
                field.get,
                fieldAccess.getField.getName,
                true)),
            parent)
        } else {
          val child = parent.children.get(fieldAccess.getField.getName)
          if (child.useAll) {
            (Some(new RexInputRef(child.order, child.fieldType)), child)
          } else {
            (Option.empty, child)
          }
        }
    }
  }
}

/**
 * An RexVisitor to extract all referenced input fields
 */
private class NestedFieldExtractor(val root: RexNodeNestedField, val rowType: RelDataType)
  extends RexVisitorImpl[Unit](true) {

  override def visitFieldAccess(fieldAccess: RexFieldAccess): Unit = {
    def internalVisit(fieldAccess: RexFieldAccess): (Int, List[String]) = {
      fieldAccess.getReferenceExpr match {
        case ref: RexInputRef =>
          (ref.getIndex, List(ref.getName, fieldAccess.getField.getName))
        case fac: RexFieldAccess =>
          val (i, n) = internalVisit(fac)
          (i, n :+ fieldAccess.getField.getName)
      }
    }

    // extract the info
    val (index, names) = internalVisit(fieldAccess)
    root.addChild(
      new RexNodeNestedField(
        names.get(0),
        index,
        rowType.getFieldList.get(index).getType,
        false,
        new util.LinkedHashMap[String, RexNodeNestedField](),
        -1))
    val (leaf, _) = names.foldLeft(Tuple2(root, rowType)) {
      case((parent, fieldType), name) =>
        if (parent.useAll) {
          return
        }
        if(!parent.children.containsKey(name)) {
          val index = fieldType.getFieldNames.indexOf(name)
          if (index < 0) {
            throw new TableException("Illegal type")
          }
          parent.addChild(
            new RexNodeNestedField(
              name,
              index,
              fieldType.getFieldList.get(index).getType,
              false,
              new util.LinkedHashMap[String, RexNodeNestedField](),
              -1
            )
          )
        }

        val son = parent.children.get(name)
        (son, fieldType.getFieldList.get(son.indexInOriginSchema).getType)
    }
    leaf.useAll = true
    leaf.children.clear()
  }

  override def visitInputRef(inputRef: RexInputRef): Unit = {
    val name = inputRef.getName
    if (root.children.containsKey(name)) {
      // mark the node as top level node
      val child = root.children.get(name)
      child.children.clear()
      child.useAll = true
    } else {
      val index = inputRef.getIndex
      root.addChild(
        new RexNodeNestedField(name,
          index,
          rowType.getFieldList.get(index).getType,
          true,
          new util.LinkedHashMap[String, RexNodeNestedField](),
          -1))

    }
  }
}
