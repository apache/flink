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

package org.apache.flink.api.table.plan.nodes.dataset

import java.lang.reflect.Field

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.{TypeInformation, AtomicType}
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.plan.TypeConverter
import org.apache.flink.api.table.plan.schema.DataSetTable
import org.apache.flink.api.table.typeinfo.RowTypeInfo
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConverters._

/**
  * Flink RelNode which matches along with DataSource.
  */
class DataSetSource(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    rowType: RelDataType)
  extends TableScan(cluster, traitSet, table)
  with DataSetRel {

  val dataSetTable: DataSetTable[Any] = table.unwrap(classOf[DataSetTable[Any]])

  override def deriveRowType() = rowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetSource(
      cluster,
      traitSet,
      table,
      rowType
    )
  }

  override def translateToPlan: DataSet[Any] = {

    val inputDataSet: DataSet[Any] = dataSetTable.dataSet

    // extract Flink data types
    val fieldTypes: Array[TypeInformation[_]] = rowType.getFieldList.asScala
      .map(f => f.getType.getSqlTypeName)
      .map(n => TypeConverter.sqlTypeToTypeInfo(n))
      .toArray

    val rowTypeInfo = new RowTypeInfo(fieldTypes, dataSetTable.fieldNames)

    // convert input data set into row data set
    inputDataSet.getType match {
      case t: TupleTypeInfo[_] =>
        val rowMapper = new TupleToRowMapper(dataSetTable.fieldIndexes)
        inputDataSet.asInstanceOf[DataSet[Tuple]]
          .map(rowMapper).returns(rowTypeInfo).asInstanceOf[DataSet[Any]]

      case c: CaseClassTypeInfo[_] =>
        val rowMapper = new CaseClassToRowMapper(dataSetTable.fieldIndexes)
        inputDataSet.asInstanceOf[DataSet[Product]]
          .map(rowMapper).returns(rowTypeInfo).asInstanceOf[DataSet[Any]]

      case p: PojoTypeInfo[_] =>
        // get pojo class
        val typeClazz = p.getTypeClass.asInstanceOf[Class[Any]]
        // get original field names
        val origFieldNames = dataSetTable.fieldIndexes.map(i => p.getFieldNames()(i))

        val rowMapper = new PojoToRowMapper(typeClazz, origFieldNames)
        inputDataSet.asInstanceOf[DataSet[Any]]
          .map(rowMapper).returns(rowTypeInfo).asInstanceOf[DataSet[Any]]

      case a: AtomicType[_] =>
        val rowMapper = new AtomicToRowMapper
        inputDataSet.asInstanceOf[DataSet[Any]]
          .map(rowMapper).returns(rowTypeInfo).asInstanceOf[DataSet[Any]]
    }
  }

}

class TupleToRowMapper(val fromIndexes: Array[Int])
  extends RichMapFunction[Tuple, Row]
{

  @transient var outR: Row = null

  override def open(conf: Configuration): Unit = {
    outR = new Row(fromIndexes.length)
  }

  override def map(v: Tuple): Row = {

    var i = 0
    while (i < fromIndexes.length) {
      outR.setField(i, v.getField(fromIndexes(i)))
      i += 1
    }
    outR
  }
}

class CaseClassToRowMapper(val fromIndexes: Array[Int])
  extends RichMapFunction[Product, Row]
{

  @transient var outR: Row = null

  override def open(conf: Configuration): Unit = {
    outR = new Row(fromIndexes.length)
  }

  override def map(v: Product): Row = {

    var i = 0
    while (i < fromIndexes.length) {
      outR.setField(i, v.productElement(fromIndexes(i)))
      i += 1
    }
    outR
  }
}

class PojoToRowMapper(val inClazz: Class[Any], val fieldNames: Array[String])
  extends RichMapFunction[Any, Row]
{

  @transient var outR: Row = null
  @transient var fields: Array[Field] = null

  override def open(conf: Configuration): Unit = {

    fields = fieldNames.map { n =>
      val f = inClazz.getField(n)
      f.setAccessible(true)
      f
    }
    outR = new Row(fieldNames.length)
  }

  override def map(v: Any): Row = {

    var i = 0
    while (i < fields.length) {
      outR.setField(i, fields(i).get(v))
      i += 1
    }
    outR
  }
}

class AtomicToRowMapper()
  extends RichMapFunction[Any, Row]
{

  @transient var outR: Row = null

  override def open(conf: Configuration): Unit = {
    outR = new Row(1)
  }

  override def map(v: Any): Row = {

    outR.setField(0, v)
    outR
  }
}
