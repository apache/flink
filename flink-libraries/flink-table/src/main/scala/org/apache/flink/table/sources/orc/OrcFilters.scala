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

package org.apache.flink.table.sources.orc

import org.apache.calcite.util.NlsString
import org.apache.flink.table.api.types._
import org.apache.flink.table.expressions.{ArrayConstructor, Expression, Literal}
import org.apache.orc.storage.ql.io.sarg.SearchArgument.Builder
import org.apache.orc.storage.ql.io.sarg.{PredicateLeaf, SearchArgument, SearchArgumentFactory}
import org.apache.orc.storage.serde2.io.HiveDecimalWritable

object OrcFilters {
  /**
    * Create ORC filter as a SearchArgument instance.
    */
  def createFilter(
      types: Array[InternalType],
      names: Array[String],
      filters: Seq[Expression]): Option[SearchArgument] = {
    val dataTypeMap = names.zip(types).toMap

    // First, tries to convert each filter individually to see whether it's convertible, and then
    // collect all convertible ones to build the final `SearchArgument`.
    val convertibleFilters = for {
      filter <- filters
      _ <- buildSearchArgument(dataTypeMap, filter, SearchArgumentFactory.newBuilder())
    } yield filter

    for {
      // Combines all convertible filters using `And` to produce a single conjunction
      conjunction <- convertibleFilters.reduceOption(org.apache.flink.table.expressions.And)
      // Then tries to build a single ORC `SearchArgument` for the conjunction predicate
      builder <- buildSearchArgument(dataTypeMap, conjunction, SearchArgumentFactory.newBuilder())
    } yield builder.build()
  }

  /**
    * Return true if this is a searchable type in ORC.
    * Both CharType and VarcharType are cleaned at AstBuilder.
    */
  private def isSearchableType(dataType: InternalType) = dataType match {
    case DataTypes.BOOLEAN => true
    case DataTypes.BYTE | DataTypes.SHORT | DataTypes.INT | DataTypes.LONG => true
    case DataTypes.FLOAT | DataTypes.DOUBLE => true
    case DataTypes.STRING => true
    case DataTypes.DATE => true
    case DataTypes.TIMESTAMP => true
    case _: DecimalType => true
    case _ => false
  }

  /**
    * Get PredicateLeafType which is corresponding to the given DataType.
    */
  private def getPredicateLeafType(dataType: InternalType) = dataType match {
    case DataTypes.BOOLEAN => PredicateLeaf.Type.BOOLEAN
    case DataTypes.BYTE | DataTypes.SHORT | DataTypes.INT | DataTypes.LONG =>
      PredicateLeaf.Type.LONG
    case DataTypes.FLOAT | DataTypes.DOUBLE => PredicateLeaf.Type.FLOAT
    case DataTypes.STRING => PredicateLeaf.Type.STRING
    case DataTypes.DATE => PredicateLeaf.Type.DATE
    case DataTypes.TIMESTAMP  => PredicateLeaf.Type.TIMESTAMP
    case _: DecimalType => PredicateLeaf.Type.DECIMAL
    case _ => throw new UnsupportedOperationException(s"DataType: $dataType")
  }

  /**
    * Cast literal values for filters.
    *
    * We need to cast to long because ORC raises exceptions
    * at 'checkLiteralType' of SearchArgumentImpl.java.
    */
  private def castLiteralValue(value: Any, dataType: InternalType): Any = dataType match {
    case DataTypes.BYTE | DataTypes.SHORT | DataTypes.INT | DataTypes.LONG =>
      value.asInstanceOf[Number].longValue
    case DataTypes.FLOAT | DataTypes.DOUBLE =>
      value.asInstanceOf[Number].doubleValue()
    case _: DecimalType =>
      val decimal = value.asInstanceOf[java.math.BigDecimal]
      val decimalWritable = new HiveDecimalWritable(decimal.longValue)
      decimalWritable.mutateEnforcePrecisionScale(decimal.precision, decimal.scale)
      decimalWritable
    case _ => value
  }

  // Currently support int/double/float/string/binary/long
  private def isLiteral(
      expression: Expression,
      option: Option[InternalType] = None): Boolean = {
    expression match {
      case Literal(_, retType) if option.isEmpty || option.get.equals(retType) => true
      case ArrayConstructor(expressions) if expressions.forall(
        ret => isLiteral(ret, Some(DataTypes.BYTE))) => true
      case _ => false
    }
  }

  private def isLiterals(
      expressions: Seq[Expression],
      option: Option[InternalType] = None): Boolean = {
    expressions.foreach( expression => {
      if (!isLiteral(expression)) {
        false
      }
    })
    true
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

  // remove the start char "'" for the expression name
  private def normalizeName(expression: Expression): String = {
    val name = expression.toString
    if (name.startsWith("'")) {
      name.substring(1, name.length)
    } else {
      name
    }
  }

  /**
    * Build a SearchArgument and return the builder so far.
    */
  private def buildSearchArgument(
      dataTypeMap: Map[String, InternalType],
      expression: Expression,
      builder: Builder): Option[Builder] = {
    def newBuilder = SearchArgumentFactory.newBuilder()

    def getType(attribute: String): PredicateLeaf.Type =
      getPredicateLeafType(dataTypeMap(attribute))

    import org.apache.flink.table.expressions._
    expression match {
      case And(left, right) =>
        // At here, it is not safe to just convert one side if we do not understand the
        // other side. Here is an example used to explain the reason.
        // Let's say we have NOT(a = 2 AND b in ('1')) and we do not understand how to
        // convert b in ('1'). If we only convert a = 2, we will end up with a filter
        // NOT(a = 2), which will generate wrong results.
        // Pushing one side of AND down is only safe to do at the top level.
        // You can see ParquetRelation's initializeLocalJobFunc method as an example.
        for {
          _ <- buildSearchArgument(dataTypeMap, left, newBuilder)
          _ <- buildSearchArgument(dataTypeMap, right, newBuilder)
          lhs <- buildSearchArgument(dataTypeMap, left, builder.startAnd())
          rhs <- buildSearchArgument(dataTypeMap, right, lhs)
        } yield rhs.end()

      case Or(left, right) =>
        for {
          _ <- buildSearchArgument(dataTypeMap, left, newBuilder)
          _ <- buildSearchArgument(dataTypeMap, right, newBuilder)
          lhs <- buildSearchArgument(dataTypeMap, left, builder.startOr())
          rhs <- buildSearchArgument(dataTypeMap, right, lhs)
        } yield rhs.end()

      case Not(child) =>
        for {
          _ <- buildSearchArgument(dataTypeMap, child, newBuilder)
          negate <- buildSearchArgument(dataTypeMap, child, builder.startNot())
        } yield negate.end()

      // NOTE: For all case branches dealing with leaf predicates below, the additional `startAnd()`
      // call is mandatory.  ORC `SearchArgument` builder requires that all leaf predicates must be
      // wrapped by a "parent" predicate (`And`, `Or`, or `Not`).

      case EqualTo(attribute, value)
          if isLiteral(value) &&
            isSearchableType(dataTypeMap.get(normalizeName(attribute)).orNull) =>
        val name = normalizeName(attribute)
        val castedValue = castLiteralValue(getLiteralValue(value), dataTypeMap(name))
        Some(builder.startAnd().equals(name, getType(name), castedValue).end())

      case LessThan(attribute, value)
          if isLiteral(value) &&
            isSearchableType(dataTypeMap.get(normalizeName(attribute)).orNull) =>
        val name = normalizeName(attribute)
        val castedValue = castLiteralValue(getLiteralValue(value), dataTypeMap(name))
        Some(builder.startAnd().lessThan(name, getType(name), castedValue).end())

      case LessThanOrEqual(attribute, value)
          if isLiteral(value) &&
            isSearchableType(dataTypeMap.get(normalizeName(attribute)).orNull) =>
        val name = normalizeName(attribute)
        val castedValue = castLiteralValue(getLiteralValue(value), dataTypeMap(name))
        Some(builder.startAnd().lessThanEquals(name, getType(name), castedValue).end())

      case GreaterThan(attribute, value)
          if isLiteral(value) &&
            isSearchableType(dataTypeMap.get(normalizeName(attribute)).orNull) =>
        val name = normalizeName(attribute)
        val castedValue = castLiteralValue(getLiteralValue(value), dataTypeMap(name))
        Some(builder.startNot().lessThanEquals(name, getType(name), castedValue).end())

      case GreaterThanOrEqual(attribute, value)
          if isLiteral(value) &&
            isSearchableType(dataTypeMap.get(normalizeName(attribute)).orNull) =>
        val name = normalizeName(attribute)
        val castedValue = castLiteralValue(getLiteralValue(value), dataTypeMap(name))
        Some(builder.startNot().lessThan(name, getType(name), castedValue).end())

      case IsNull(attribute)
          if isSearchableType(dataTypeMap.get(normalizeName(attribute)).orNull) =>
        val name = normalizeName(attribute)
        Some(builder.startAnd().isNull(name, getType(name)).end())

      case IsNotNull(attribute)
          if isSearchableType(dataTypeMap.get(normalizeName(attribute)).orNull) =>
        val name = normalizeName(attribute)
        Some(builder.startNot().isNull(name, getType(name)).end())

      case In(attribute, values)
          if isLiterals(values) &&
            isSearchableType(dataTypeMap.get(normalizeName(attribute)).orNull) =>
        val name = normalizeName(attribute)
        val castedValues = values.map(v => castLiteralValue(getLiteralValue(v), dataTypeMap(name)))
        Some(builder.startAnd().in(name, getType(name),
          castedValues.map(_.asInstanceOf[AnyRef]): _*).end())

      case _ => None
    }
  }
}
