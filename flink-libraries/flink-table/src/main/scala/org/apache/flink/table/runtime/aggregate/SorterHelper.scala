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
package org.apache.flink.table.runtime.aggregate

import java.util.Comparator
import org.apache.calcite.rel.RelFieldCollation
import org.apache.flink.api.common.functions.{Comparator => FlinkComparator}
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}
import org.apache.flink.table.api.types.{DataTypes, InternalType, RowType}
import org.apache.flink.table.codegen.{CodeGenUtils, GeneratedSorter, SortCodeGenerator}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.util.SortUtil
import org.apache.flink.table.runtime.sort.RecordComparator
import org.apache.flink.table.typeutils.TypeUtils

/**
 * Class represents a collection of helper methods to build the sort logic.
 * It encapsulates as well the implementation for ordering and generic interfaces
 */
object SorterHelper {

  /**
    * Creates a GeneratedSorter for the provided field collations and input type.
    *
    * @param inputType the row type of the input.
    * @param fieldCollations the field collations
    *
    * @return A GeneratedSorter for the provided sort collations and input type.
    */
  def createSorter(
      inputType: RowType,
      fieldCollations: Seq[RelFieldCollation]): GeneratedSorter = {
    val (sortFields, sortDirections, nullsIsLast) = SortUtil.getKeysAndOrders(fieldCollations)
    createSorter(
      inputType.getFieldTypes.map(_.toInternalType), sortFields, sortDirections, nullsIsLast)
  }

  def createSorter(
      fieldTypes: Array[InternalType],
      sortFields: Array[Int],
      sortDirections: Array[Boolean],
      nullsIsLast: Array[Boolean]): GeneratedSorter = {
    // sort code gen
    val (comparators, serializers) = TypeUtils.flattenComparatorAndSerializer(
      fieldTypes.length, sortFields, sortDirections, fieldTypes)
    val codeGen = new SortCodeGenerator(sortFields, sortFields.map((key) =>
      fieldTypes(key)).map(_.toInternalType), comparators, sortDirections, nullsIsLast)

    val comparator = codeGen.generateRecordComparator("StreamExecSortComparator")
    val computer = codeGen.generateNormalizedKeyComputer("StreamExecSortComputer")
    GeneratedSorter(computer, comparator, serializers, comparators)
  }

}

/**
 * Wrapper for RecordComparator to a Java Comparator object
 */
class CollectionBaseRowComparator(
    private val rowComp: RecordComparator) extends Comparator[BaseRow] with Serializable {

  override def compare(arg0: BaseRow, arg1: BaseRow):Int = {
    rowComp.compare(arg0, arg1)
  }
}

class LazyBaseRowComparator(
    private val name: String,
    private val code: String,
    private val serializers: Array[TypeSerializer[_]],
    private val comparators: Array[TypeComparator[_]])
  extends FlinkComparator[BaseRow]
  with Serializable {

  @transient
  private var comparator: RecordComparator = _

  override def compare(o1: BaseRow, o2: BaseRow): Int = {
    if (comparator == null) {
      val clazz = CodeGenUtils.compile(Thread.currentThread().getContextClassLoader, name, code)
      comparator = clazz.newInstance()
      comparator.init(serializers, comparators)
    }
    comparator.compare(o1, o2)
  }

  override def equals(other: Any): Boolean = other match {
    case that: LazyBaseRowComparator =>
      this.name == that.name && this.code == that.code &&
        this.serializers.sameElements(that.serializers) &&
        this.comparators.sameElements(that.comparators)
    case _ => false
  }
}
