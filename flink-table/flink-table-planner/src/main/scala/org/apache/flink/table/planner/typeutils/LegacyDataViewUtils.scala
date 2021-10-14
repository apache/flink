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
package org.apache.flink.table.planner.typeutils

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{PojoField, PojoTypeInfo}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.dataview._
import org.apache.flink.table.data.binary.BinaryRawValueData
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.dataview.{ListViewTypeInfo, MapViewTypeInfo}
import org.apache.flink.table.functions.ImperativeAggregateFunction
import org.apache.flink.table.planner.typeutils.DataViewUtils.{DataViewSpec, ListViewSpec, MapViewSpec}
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.{fromLogicalTypeToTypeInfo, fromTypeInfoToLogicalType}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LegacyTypeInformationType
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType

import java.util

import scala.collection.mutable

object LegacyDataViewUtils {

  /**
    * Use NullSerializer for StateView fields from accumulator type information.
    *
    * @param index index of aggregate function
    * @param aggFun aggregate or table aggregate function
    * @param externalAccType accumulator type information, only support pojo type
    * @param isStateBackedDataViews is data views use state backend
    * @return mapping of accumulator type information and data view config which contains id,
    *         field name and state descriptor
    */
  def useNullSerializerForStateViewFieldsFromAccType(
      index: Int,
      aggFun: ImperativeAggregateFunction[_, _],
      externalAccType: DataType,
      isStateBackedDataViews: Boolean)
    : (DataType, Array[DataViewSpec]) = {

    val acc = aggFun.createAccumulator()
    val accumulatorSpecs = new mutable.ArrayBuffer[DataViewSpec]

    externalAccType.getLogicalType match {
      case t: LegacyTypeInformationType[_] =>
        t.getTypeInformation match {
          case pojoType: PojoTypeInfo[_] if pojoType.getArity > 0 =>
            val arity = pojoType.getArity
            val newPojoFields = new util.ArrayList[PojoField]()

            for (i <- 0 until arity) {
              val pojoField = pojoType.getPojoFieldAt(i)
              val field = pojoField.getField
              val fieldName = field.getName
              field.setAccessible(true)
              val instance = field.get(acc)
              val (newTypeInfo: TypeInformation[_], spec: Option[DataViewSpec]) =
                decorateDataViewTypeInfo(
                  pojoField.getTypeInformation,
                  instance,
                  isStateBackedDataViews,
                  index,
                  i,
                  fieldName)

              newPojoFields.add(new PojoField(field, newTypeInfo))
              if (spec.isDefined) {
                accumulatorSpecs += spec.get
              }
            }
            val pojoTypeInfo = new PojoTypeInfo(pojoType.getTypeClass, newPojoFields)
            (fromLegacyInfoToDataType(pojoTypeInfo), accumulatorSpecs.toArray)

          // so we add another check => acc.isInstanceOf[GenericRowData]
          case t: InternalTypeInfo[RowData] if acc.isInstanceOf[GenericRowData] =>
            val accInstance = acc.asInstanceOf[GenericRowData]
            val (arity, fieldNames, fieldTypes) = (
              t.toRowSize,
              t.toRowFieldNames,
              t.toRowFieldTypes)
            val newFieldTypes = for (i <- 0 until arity) yield {
              val fieldName = fieldNames(i)
              val fieldInstance = accInstance.getField(i)
              val (newTypeInfo: TypeInformation[_], spec: Option[DataViewSpec]) =
                decorateDataViewTypeInfo(
                  fromLogicalTypeToTypeInfo(fieldTypes(i)),
                  fieldInstance,
                  isStateBackedDataViews,
                  index,
                  i,
                  fieldName)
              if (spec.isDefined) {
                accumulatorSpecs += spec.get
              }
              fromTypeInfoToLogicalType(newTypeInfo)
            }

            val newType = InternalTypeInfo.ofFields(newFieldTypes.toArray, fieldNames)
            (fromLegacyInfoToDataType(newType), accumulatorSpecs.toArray)

          case ct: CompositeType[_] if includesDataView(ct) =>
            throw new TableException(
              "MapView, SortedMapView and ListView only supported in accumulators of POJO type.")
          case _ => (externalAccType, Array.empty)
        }
      case _ => (externalAccType, Array.empty)
    }
  }

  /** Recursively checks if composite type includes a data view type. */
  def includesDataView(ct: CompositeType[_]): Boolean = {
    (0 until ct.getArity).exists(i =>
      ct.getTypeAt(i) match {
        case nestedCT: CompositeType[_] => includesDataView(nestedCT)
        case t: TypeInformation[_] if t.getTypeClass == classOf[ListView[_]] => true
        case t: TypeInformation[_] if t.getTypeClass == classOf[MapView[_, _]] => true
        // TODO supports SortedMapView
        // case t: TypeInformation[_] if t.getTypeClass == classOf[SortedMapView[_, _]] => true
        case _ => false
      }
    )
  }

  /** Analyse dataview element types and decorate the dataview typeinfos */
  def decorateDataViewTypeInfo(
      info: TypeInformation[_],
      instance: AnyRef,
      isStateBackedDataViews: Boolean,
      aggIndex: Int,
      fieldIndex: Int,
      fieldName: String): (TypeInformation[_], Option[DataViewSpec]) = {
    var spec: Option[DataViewSpec] = None
    val resultTypeInfo: TypeInformation[_] = info match {
      case ct: CompositeType[_] if includesDataView(ct) =>
        throw new TableException(
          "MapView, SortedMapView and ListView only supported at first level of " +
            "accumulators of Pojo type.")
      case map: MapViewTypeInfo[_, _] =>
        val mapView = instance match {
          case b: BinaryRawValueData[_] =>
            b.getJavaObject.asInstanceOf[MapView[_, _]]
          case _ =>
            instance.asInstanceOf[MapView[_, _]]
        }

        val newTypeInfo =
          if (mapView != null && mapView.keyType != null && mapView.valueType != null) {
            // use explicit key value type if user has defined
            new MapViewTypeInfo(mapView.keyType, mapView.valueType)
          } else {
            map
          }

        if (!isStateBackedDataViews) {
          // add data view field if it is not backed by a state backend.
          // data view fields which are backed by state backend are not serialized.
          newTypeInfo.setNullSerializer(false)
        } else {
          newTypeInfo.setNullSerializer(true)

          // create map view specs with unique id (used as state name)
          val mapViewSpec = new MapViewSpec(
            "agg" + aggIndex + "$" + fieldName,
            fieldIndex, // dataview field index in pojo
            fromLegacyInfoToDataType(newTypeInfo),
            false,
            newTypeInfo.getKeyType.createSerializer(new ExecutionConfig),
            newTypeInfo.getValueType.createSerializer(new ExecutionConfig)
          )
          spec = Some(mapViewSpec)
        }
        newTypeInfo

      case list: ListViewTypeInfo[_] =>
        val listView = instance match {
          case b: BinaryRawValueData[_] =>
            b.getJavaObject.asInstanceOf[ListView[_]]
          case _ =>
            instance.asInstanceOf[ListView[_]]
        }
        val newTypeInfo = if (listView != null && listView.elementType != null) {
          // use explicit element type if user has defined
          new ListViewTypeInfo(listView.elementType)
        } else {
          list
        }
        if (!isStateBackedDataViews) {
          // add data view field if it is not backed by a state backend.
          // data view fields which are backed by state backend are not serialized.
          newTypeInfo.setNullSerializer(false)
        } else {
          newTypeInfo.setNullSerializer(true)

          // create list view specs with unique is (used as state name)
          val listViewSpec = new ListViewSpec(
            "agg" + aggIndex + "$" + fieldName,
            fieldIndex, // dataview field index in pojo
            fromLegacyInfoToDataType(newTypeInfo),
            newTypeInfo.getElementType.createSerializer(new ExecutionConfig)
          )
          spec = Some(listViewSpec)
        }
        newTypeInfo

      case t: TypeInformation[_] => t
    }

    (resultTypeInfo, spec)
  }

}
