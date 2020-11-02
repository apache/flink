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
 * @param isLeaf               Mark the field is the leaf node in the tree.
 * @param children             Store the children of the field. It's safe
 *                             to use name as the index because name is
 *                             unique in every level. It uses the
 *                             LinkedHashMap to keep the insert order.
 *                             In some cases, it can reduce the cost of the
 *                             reorder of the fields in query.
 * @param indexInNewSchema     It is used by the leaf node to memorize the
 *                             index in the new schema.
 */
class RexNodeNestedField(
    val name: String,
    val indexInOriginSchema: Int,
    val fieldType: RelDataType,
    val children: util.LinkedHashMap[String, RexNodeNestedField],
    var isLeaf: Boolean,
    var indexInNewSchema: Int) {

  def addChild(field: RexNodeNestedField): Unit = {
    if (!children.contains(field.name)) {
      isLeaf = false
      children.put(field.name, field)
    }
  }
}

/**
 * RexNodeNestedFields could be regard as a table schema that represents
 * a table's structure with field names and data types. It uses a
 * LinkedHashMap to store the pairs of name: String and column: RexNodeNestedField.
 *
 * @param columns  Fields in the origin schema are used by the query.
 */
class RexNodeNestedFields(
    val columns: util.LinkedHashMap[String, RexNodeNestedField]) {

}

object RexNodeNestedFields {
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
      RexNodeNestedFields = {
    // the order field in the root node is to memorize
    // the number of leaf
    val root = new RexNodeNestedField(
      "root",
      -1,
      rowType,
      new util.LinkedHashMap[String, RexNodeNestedField](),
      false,
      -1)
    val visitor = new NestedFieldExtractor(root, rowType)
    for(expr <- exprs) {
      expr.accept(visitor)
    }
    new RexNodeNestedFields(root.children)
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
      root: RexNodeNestedFields,
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
  def labelAndConvert(root: RexNodeNestedFields): Array[Array[Int]] = {
    val allPaths = new util.LinkedList[Array[Int]]()
    val path = new util.LinkedList[Int]()
    root.columns.foldLeft(0) {
      case (newOrder, (_, column)) =>
        traverse(column, newOrder, path, allPaths)
    }
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
    val newOrder = if (parent.isLeaf) {
      // leaf node
      parent.indexInNewSchema = order
      // ignore root node
      allPaths.add(path.asScala.toArray)
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
    usedFields: RexNodeNestedFields,
    builder: RexBuilder) extends RexShuttle {
  override def visitInputRef(inputRef: RexInputRef): RexNode = {
    if (!usedFields.columns.containsKey(inputRef.getName)) {
      throw new TableException(
        "Illegal input field access" + inputRef.getName)
    } else {
      val field = usedFields.columns.get(inputRef.getName)
      new RexInputRef(field.indexInNewSchema, field.fieldType)
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
        val parent = usedFields.columns.get(ref.getName)
        if (parent.isLeaf) {
          (
            Some(builder.makeFieldAccess(
              new RexInputRef(parent.indexInNewSchema, parent.fieldType),
              fieldAccess.getField.getName,
              true)), parent)
        } else {
          val child = parent.children.get(fieldAccess.getField.getName)
          if (child.isLeaf) {
            (Some(new RexInputRef(child.indexInNewSchema, child.fieldType)), child)
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
          if (child.isLeaf) {
            (Some(new RexInputRef(child.indexInNewSchema, child.fieldType)), child)
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
        new util.LinkedHashMap[String, RexNodeNestedField](),
        false,
        -1))
    val (leaf, _) = names.foldLeft(Tuple2(root, rowType)) {
      case((parent, fieldType), name) =>
        if (parent.isLeaf) {
          return
        }
        if(!parent.children.containsKey(name)) {
          val index = fieldType.getFieldNames.indexOf(name)
          if (index < 0) {
            throw new TableException(
              String.format("Could not find filed %s in field %s.", name, parent.fieldType))
          }
          parent.addChild(
            new RexNodeNestedField(
              name,
              index,
              fieldType.getFieldList.get(index).getType,
              new util.LinkedHashMap[String, RexNodeNestedField](),
              false,
              -1
            )
          )
        }

        val son = parent.children.get(name)
        (son, fieldType.getFieldList.get(son.indexInOriginSchema).getType)
    }
    leaf.isLeaf = true
    leaf.children.clear()
  }

  override def visitInputRef(inputRef: RexInputRef): Unit = {
    val name = inputRef.getName
    if (root.children.containsKey(name)) {
      // mark the node as top level node
      val child = root.children.get(name)
      child.children.clear()
      child.isLeaf = true
    } else {
      val index = inputRef.getIndex
      root.addChild(
        new RexNodeNestedField(name,
          index,
          rowType.getFieldList.get(index).getType,
          new util.LinkedHashMap[String, RexNodeNestedField](),
          true,
          -1))
    }
  }
}
