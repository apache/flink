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

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInteger, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Time, Timestamp}
import org.apache.flink.table.api.scala._

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.typeutils.{RowIntervalTypeInfo, TimeIntervalTypeInfo}

import scala.collection.JavaConverters._

object ExpressionUtils {
  private[flink] def call(func: FunctionDefinition, args: Seq[Expression]): CallExpression = {
    new CallExpression(func, args.asJava)
  }

  private[flink] def aggCall(
    func: FunctionDefinition, args: Seq[Expression]): AggregateCallExpression = {
    new AggregateCallExpression(func, args.asJava)
  }

  private[flink] def literal(l: Any): ValueLiteralExpression = {
    new ValueLiteralExpression(l)
  }

  private[flink] def literal(l: Any, t: TypeInformation[_]): ValueLiteralExpression = {
    new ValueLiteralExpression(l, t)
  }

  private[flink] def toMonthInterval(expr: Expression, multiplier: Int): Expression =
    expr match {
      case e: ValueLiteralExpression if e.getValue.isInstanceOf[Int] && !e.getType.isPresent =>
        literal(e.getValue.asInstanceOf[Int] * multiplier,
          TimeIntervalTypeInfo.INTERVAL_MONTHS)
      case e: ValueLiteralExpression if e.getValue.isInstanceOf[Int] && e.getType.isPresent
        && e.getType.get().equals(BasicTypeInfo.INT_TYPE_INFO) =>
        literal(e.getValue.asInstanceOf[Int] * multiplier,
          TimeIntervalTypeInfo.INTERVAL_MONTHS)
      case _ =>
        call(FunctionDefinitions.CAST, Seq(
          call(FunctionDefinitions.TIMES, Seq(expr, literal(multiplier))),
          TimeIntervalTypeInfo.INTERVAL_MONTHS))
  }

  private[flink] def toMilliInterval(expr: Expression, multiplier: Long): Expression =
    expr match {
      case e: ValueLiteralExpression if e.getValue.isInstanceOf[Int] && !e.getType.isPresent =>
        literal(e.getValue.asInstanceOf[Int] * multiplier,
          TimeIntervalTypeInfo.INTERVAL_MILLIS)
      case e: ValueLiteralExpression if e.getValue.isInstanceOf[Int] && e.getType.isPresent
        && e.getType.get().equals(BasicTypeInfo.INT_TYPE_INFO) =>
        literal(e.getValue.asInstanceOf[Int] * multiplier,
          TimeIntervalTypeInfo.INTERVAL_MILLIS)
      case e: ValueLiteralExpression if e.getValue.isInstanceOf[Long] && !e.getType.isPresent =>
        literal(e.getValue.asInstanceOf[Long] * multiplier,
          TimeIntervalTypeInfo.INTERVAL_MILLIS)
      case e: ValueLiteralExpression if e.getValue.isInstanceOf[Long] && e.getType.isPresent
        && e.getType.get().equals(BasicTypeInfo.LONG_TYPE_INFO) =>
        literal(e.getValue.asInstanceOf[Long] * multiplier,
          TimeIntervalTypeInfo.INTERVAL_MILLIS)
      case _ =>
        call(FunctionDefinitions.CAST, Seq(
          call(FunctionDefinitions.TIMES, Seq(expr, literal(multiplier))),
          TimeIntervalTypeInfo.INTERVAL_MILLIS))
    }

  private[flink] def toRowInterval(expr: Expression): Expression =
    expr match {
      case e: ValueLiteralExpression if e.getValue.isInstanceOf[Int] && !e.getType.isPresent =>
        literal(e.getValue.asInstanceOf[Int].toLong,
          RowIntervalTypeInfo.INTERVAL_ROWS)
      case e: ValueLiteralExpression if e.getValue.isInstanceOf[Int] && e.getType.isPresent
        && e.getType.get().equals(BasicTypeInfo.INT_TYPE_INFO) =>
        literal(e.getValue.asInstanceOf[Int].toLong,
          RowIntervalTypeInfo.INTERVAL_ROWS)
      case e: ValueLiteralExpression if e.getValue.isInstanceOf[Long] && !e.getType.isPresent =>
        literal(e.getValue, RowIntervalTypeInfo.INTERVAL_ROWS)
      case e: ValueLiteralExpression if e.getValue.isInstanceOf[Long] && e.getType.isPresent
        && e.getType.get().equals(BasicTypeInfo.LONG_TYPE_INFO) =>
        literal(e.getValue, RowIntervalTypeInfo.INTERVAL_ROWS)
    }

  private[flink] def convertArray(array: Array[_]): Expression = {
    def createArray(): Expression = {
      call(FunctionDefinitions.ARRAY,
        array.map(literal(_).asInstanceOf[Expression]))
    }

    array match {
      // primitives
      case _: Array[Boolean] => createArray()
      case _: Array[Byte] => createArray()
      case _: Array[Short] => createArray()
      case _: Array[Int] => createArray()
      case _: Array[Long] => createArray()
      case _: Array[Float] => createArray()
      case _: Array[Double] => createArray()

      // boxed types
      case _: Array[JBoolean] => createArray()
      case _: Array[JByte] => createArray()
      case _: Array[JShort] => createArray()
      case _: Array[JInteger] => createArray()
      case _: Array[JLong] => createArray()
      case _: Array[JFloat] => createArray()
      case _: Array[JDouble] => createArray()

      // others
      case _: Array[String] => createArray()
      case _: Array[JBigDecimal] => createArray()
      case _: Array[Date] => createArray()
      case _: Array[Time] => createArray()
      case _: Array[Timestamp] => createArray()
      case bda: Array[BigDecimal] =>
        call(FunctionDefinitions.ARRAY,
          bda.map { bd => literal(bd.bigDecimal)})

      case _ =>
        // nested
        if (array.length > 0 && array.head.isInstanceOf[Array[_]]) {
          call(FunctionDefinitions.ARRAY,
            array.map { na => convertArray(na.asInstanceOf[Array[_]]) })
        } else {
          throw new ValidationException("Unsupported array type.")
        }
    }
  }
}

