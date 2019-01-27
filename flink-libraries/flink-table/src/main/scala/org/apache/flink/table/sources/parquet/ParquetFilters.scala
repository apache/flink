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

package org.apache.flink.table.sources.parquet

import org.apache.calcite.util.NlsString
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.expressions._
import org.apache.parquet.filter2.predicate.FilterApi._
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.io.api.Binary

/**
  * Some utility function to convert Flink data source filters to Parquet filters.
  */
object ParquetFilters {

  def applyPredicate(
      types: Array[InternalType],
      names: Array[String],
      filters: Array[Expression]): FilterPredicate = {
    val dataTypes = getFieldMap(types, names)
    filters.flatMap(createFilter(dataTypes, _)).reduceOption(FilterApi.and).orNull
  }

  private val makeEq: PartialFunction[InternalType, (String, Any) => FilterPredicate] = {
    case DataTypes.BOOLEAN =>
      (n: String, v: Any) => FilterApi.eq(booleanColumn(n), v.asInstanceOf[java.lang.Boolean])
    case DataTypes.INT =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].intValue
        FilterApi.eq(intColumn(n), value.asInstanceOf[java.lang.Integer])
    case DataTypes.LONG =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].longValue
        FilterApi.eq(longColumn(n), value.asInstanceOf[java.lang.Long])
    case DataTypes.FLOAT =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].floatValue
        FilterApi.eq(floatColumn(n), value.asInstanceOf[java.lang.Float])
    case DataTypes.DOUBLE =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].doubleValue
        FilterApi.eq(doubleColumn(n), value.asInstanceOf[java.lang.Double])

    // Binary.fromString and Binary.fromByteArray don't accept null values
    case DataTypes.STRING =>
      (n: String, v: Any) => FilterApi.eq(
        binaryColumn(n),
        Option(v).map(s => Binary.fromString(s.asInstanceOf[String])).orNull)
    case DataTypes.BYTE_ARRAY =>
      (n: String, v: Any) => FilterApi.eq(
        binaryColumn(n),
        Option(v).map(b => Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]])).orNull)
  }

  private val makeNotEq: PartialFunction[InternalType, (String, Any) => FilterPredicate] = {
    case DataTypes.BOOLEAN =>
      (n: String, v: Any) => FilterApi.notEq(booleanColumn(n), v.asInstanceOf[java.lang.Boolean])
    case DataTypes.INT =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].intValue
        FilterApi.notEq(intColumn(n), value.asInstanceOf[java.lang.Integer])
    case DataTypes.LONG =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].longValue
        FilterApi.notEq(longColumn(n), value.asInstanceOf[java.lang.Long])
    case DataTypes.FLOAT =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].floatValue
        FilterApi.notEq(floatColumn(n), value.asInstanceOf[java.lang.Float])
    case DataTypes.DOUBLE =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].doubleValue
        FilterApi.notEq(doubleColumn(n), value.asInstanceOf[java.lang.Double])

    case DataTypes.STRING =>
      (n: String, v: Any) => FilterApi.notEq(
        binaryColumn(n),
        Option(v).map(s => Binary.fromString(s.asInstanceOf[String])).orNull)
    case DataTypes.BYTE_ARRAY =>
      (n: String, v: Any) => FilterApi.notEq(
        binaryColumn(n),
        Option(v).map(b => Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]])).orNull)
  }

  private val makeLt: PartialFunction[InternalType, (String, Any) => FilterPredicate] = {
    case DataTypes.INT =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].intValue
        FilterApi.lt(intColumn(n), value.asInstanceOf[java.lang.Integer])
    case DataTypes.LONG =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].longValue
        FilterApi.lt(longColumn(n), value.asInstanceOf[java.lang.Long])
    case DataTypes.FLOAT =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].floatValue
        FilterApi.lt(floatColumn(n), value.asInstanceOf[java.lang.Float])
    case DataTypes.DOUBLE =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].doubleValue
        FilterApi.lt(doubleColumn(n), value.asInstanceOf[java.lang.Double])

    case DataTypes.STRING =>
      (n: String, v: Any) =>
        FilterApi.lt(
          binaryColumn(n),
          Binary.fromString(v.asInstanceOf[String]))
    case DataTypes.BYTE_ARRAY =>
      (n: String, v: Any) =>
        FilterApi.lt(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
  }

  private val makeLtEq: PartialFunction[InternalType, (String, Any) => FilterPredicate] = {
    case DataTypes.INT =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].intValue
        FilterApi.ltEq(intColumn(n), value.asInstanceOf[java.lang.Integer])
    case DataTypes.LONG =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].longValue
        FilterApi.ltEq(longColumn(n), value.asInstanceOf[java.lang.Long])
    case DataTypes.FLOAT =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].floatValue
        FilterApi.ltEq(floatColumn(n), value.asInstanceOf[java.lang.Float])
    case DataTypes.DOUBLE =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].doubleValue
        FilterApi.ltEq(doubleColumn(n), value.asInstanceOf[java.lang.Double])

    case DataTypes.STRING =>
      (n: String, v: Any) =>
        FilterApi.ltEq(
          binaryColumn(n),
          Binary.fromString(v.asInstanceOf[String]))
    case DataTypes.BYTE_ARRAY =>
      (n: String, v: Any) =>
        FilterApi.ltEq(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
  }

  private val makeGt: PartialFunction[InternalType, (String, Any) => FilterPredicate] = {
    case DataTypes.INT =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].intValue
        FilterApi.gt(intColumn(n), value.asInstanceOf[java.lang.Integer])
    case DataTypes.LONG =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].longValue
        FilterApi.gt(longColumn(n), value.asInstanceOf[java.lang.Long])
    case DataTypes.FLOAT =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].floatValue
        FilterApi.gt(floatColumn(n), value.asInstanceOf[java.lang.Float])
    case DataTypes.DOUBLE =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].doubleValue
        FilterApi.gt(doubleColumn(n), value.asInstanceOf[java.lang.Double])

    case DataTypes.STRING =>
      (n: String, v: Any) =>
        FilterApi.gt(
          binaryColumn(n),
          Binary.fromString(v.asInstanceOf[String]))
    case DataTypes.BYTE_ARRAY =>
      (n: String, v: Any) =>
        FilterApi.gt(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
  }

  private val makeGtEq: PartialFunction[InternalType, (String, Any) => FilterPredicate] = {
    case DataTypes.INT =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].intValue
        FilterApi.gtEq(intColumn(n), value.asInstanceOf[java.lang.Integer])
    case DataTypes.LONG =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].longValue
        FilterApi.gtEq(longColumn(n), value.asInstanceOf[java.lang.Long])
    case DataTypes.FLOAT =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].floatValue
        FilterApi.gtEq(floatColumn(n), value.asInstanceOf[java.lang.Float])
    case DataTypes.DOUBLE =>
      (n: String, v: Any) =>
        val value = if (v == null) v else v.asInstanceOf[Number].doubleValue
        FilterApi.gtEq(doubleColumn(n), value.asInstanceOf[java.lang.Double])

    case DataTypes.STRING =>
      (n: String, v: Any) =>
        FilterApi.gtEq(
          binaryColumn(n),
          Binary.fromString(v.asInstanceOf[String]))
    case DataTypes.BYTE_ARRAY =>
      (n: String, v: Any) =>
        FilterApi.gtEq(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
  }

  private def getFieldMap(
      types: Array[InternalType],
      names: Array[String]): Map[String, InternalType] = names.zip(types).toMap

  // remove the start char "'" for the expression name
  private def normalizeName(expression: Expression): String = {
    val name = expression.toString
    if (name.startsWith("'")) {
      name.substring(1, name.length)
    } else {
      name
    }
  }

  // Currently support int/double/float/string/binary/long
  private def isLiteral(
      expression: Expression,
      option: Option[InternalType] = None): Boolean = {
    expression match {
      case Literal(_, retType) if option.isEmpty || option.get.toInternalType == retType => true
      case ArrayConstructor(expressions) if expressions.forall(
        ret => isLiteral(ret, Some(DataTypes.BYTE))) => true
      case _ => false
    }
  }

  private def getLiteralValue(expression: Expression): Any = {
    expression match {
      case Literal(value, _) => {
        if (value.isInstanceOf[NlsString]) value.asInstanceOf[NlsString].getValue else value
      }
      case ArrayConstructor(expressions) =>
        expressions.map {
          case Literal(value, _) => value.asInstanceOf[Byte]
        }.toArray
      case _ => null
    }
  }

  /**
    * Converts data sources filters to Parquet filter predicates.
    */
  private def createFilter(dataTypeOf: Map[String, InternalType], predicate: Expression):
  Option[FilterPredicate] = {
    predicate match {
      case IsNull(name) if dataTypeOf.contains(normalizeName(name)) =>
        val normalName = normalizeName(name)
        makeEq.lift(dataTypeOf(normalName)).map(_ (normalName, null))

      case IsNotNull(name) if dataTypeOf.contains(normalizeName(name)) =>
        val normalName = normalizeName(name)
        makeNotEq.lift(dataTypeOf(normalName)).map(_ (normalName, null))

      case IsTrue(name) if dataTypeOf.contains(normalizeName(name)) =>
        val normalName = normalizeName(name)
        makeEq.lift(dataTypeOf(normalName)).map(_ (normalName, true))

      case IsFalse(name) if dataTypeOf.contains(normalizeName(name)) =>
        val normalName = normalizeName(name)
        makeEq.lift(dataTypeOf(normalName)).map(_ (normalName, false))

      case IsNotTrue(name) if dataTypeOf.contains(normalizeName(name)) =>
        val normalName = normalizeName(name)
        makeNotEq.lift(dataTypeOf(normalName)).map(_ (normalName, true))

      case IsNotFalse(name) if dataTypeOf.contains(normalizeName(name)) =>
        val normalName = normalizeName(name)
        makeNotEq.lift(dataTypeOf(normalName)).map(_ (normalName, false))

      case EqualTo(name, right) if isLiteral(right) && dataTypeOf.contains(normalizeName(name)) =>
        val normalName = normalizeName(name)
        makeEq.lift(dataTypeOf(normalName)).map(_ (normalName, getLiteralValue(right)))

      case EqualTo(left, name) if isLiteral(left) && dataTypeOf.contains(normalizeName(name)) =>
        val normalName = normalizeName(name)
        makeEq.lift(dataTypeOf(normalName)).map(_ (normalName, getLiteralValue(left)))

      case NotEqualTo(name, right) if isLiteral(right) &&
        dataTypeOf.contains(normalizeName(name)) =>
        val normalName = normalizeName(name)
        makeNotEq.lift(dataTypeOf(normalName)).map(_ (normalName, getLiteralValue(right)))

      case NotEqualTo(left, name) if isLiteral(left) && dataTypeOf.contains(normalizeName(name)) =>
        val normalName = normalizeName(name)
        makeNotEq.lift(dataTypeOf(normalName)).map(_ (normalName, getLiteralValue(left)))

      case LessThan(name, right) if isLiteral(right) && dataTypeOf.contains(normalizeName(name)) =>
        val normalName = normalizeName(name)
        makeLt.lift(dataTypeOf(normalName)).map(_ (normalName, getLiteralValue(right)))

      case LessThan(left, name) if isLiteral(left) && dataTypeOf.contains(normalizeName(name)) =>
        val normalName = normalizeName(name)
        makeGt.lift(dataTypeOf(normalName)).map(_ (normalName, getLiteralValue(left)))

      case LessThanOrEqual(name, right) if isLiteral(right) &&
        dataTypeOf.contains(normalizeName(name)) =>
        val normalName = normalizeName(name)
        makeLtEq.lift(dataTypeOf(normalName)).map(_ (normalName, getLiteralValue(right)))

      case LessThanOrEqual(left, name) if isLiteral(left) &&
        dataTypeOf.contains(normalizeName(name)) =>
        val normalName = normalizeName(name)
        makeGtEq.lift(dataTypeOf(normalName)).map(_ (normalName, getLiteralValue(left)))

      case GreaterThan(name, right) if isLiteral(right) &&
        dataTypeOf.contains(normalizeName(name)) =>
        val normalName = normalizeName(name)
        makeGt.lift(dataTypeOf(normalName)).map(_ (normalName, getLiteralValue(right)))

      case GreaterThan(left, name) if isLiteral(left) && dataTypeOf.contains(normalizeName(name)) =>
        val normalName = normalizeName(name)
        makeLt.lift(dataTypeOf(normalName)).map(_ (normalName, getLiteralValue(left)))

      case GreaterThanOrEqual(name, right) if isLiteral(right) &&
        dataTypeOf.contains(normalizeName(name)) =>
        val normalName = normalizeName(name)
        makeGtEq.lift(dataTypeOf(normalName)).map(_ (normalName, getLiteralValue(right)))

      case GreaterThanOrEqual(left, name) if isLiteral(left) &&
        dataTypeOf.contains(normalizeName(name)) =>
        val normalName = normalizeName(name)
        makeLtEq.lift(dataTypeOf(normalName)).map(_ (normalName, getLiteralValue(left)))

      case And(lhs, rhs) =>
        for {
          lhsFilter <- createFilter(dataTypeOf, lhs)
          rhsFilter <- createFilter(dataTypeOf, rhs)
        } yield FilterApi.and(lhsFilter, rhsFilter)

      case Or(lhs, rhs) =>
        for {
          lhsFilter <- createFilter(dataTypeOf, lhs)
          rhsFilter <- createFilter(dataTypeOf, rhs)
        } yield FilterApi.or(lhsFilter, rhsFilter)

      case Not(pred) =>
        createFilter(dataTypeOf, pred).map(FilterApi.not)

      case _ => None
    }
  }
}


