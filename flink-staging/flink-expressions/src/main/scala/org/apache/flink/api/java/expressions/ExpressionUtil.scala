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
package org.apache.flink.api.java.expressions

import org.apache.flink.api.expressions.ExpressionOperation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.scala.expressions.JavaBatchTranslator
import org.apache.flink.api.scala.expressions.JavaStreamingTranslator
import org.apache.flink.streaming.api.datastream.DataStream

/**
 * Convencience methods for creating an [[org.apache.flink.api.expressions.ExpressionOperation]]
 * and for converting an [[org.apache.flink.api.expressions.ExpressionOperation]] back
 * to a [[org.apache.flink.api.java.DataSet]] or
 * [[org.apache.flink.streaming.api.datastream.DataStream]].
 */
object ExpressionUtil {
  
  /**
   * Transforms the given DataSet to a [[org.apache.flink.api.expressions.ExpressionOperation]].
   * The fields of the DataSet type are renamed to the given set of fields:
   *
   * Example:
   *
   * {{{
   *   ExpressionUtil.from(set, "a, b")
   * }}}
   *
   * This will transform the set containing elements of two fields to a table where the fields
   * are named a and b.
   */
  def from[T](set: DataSet[T], fields: String): ExpressionOperation[JavaBatchTranslator] = {
    new JavaBatchTranslator().createExpressionOperation(set, fields)
  }

  /**
   * Transforms the given DataSet to a [[org.apache.flink.api.expressions.ExpressionOperation]].
   * The fields of the DataSet type are used to name the
   * [[org.apache.flink.api.expressions.ExpressionOperation]] fields.
   */
  def from[T](set: DataSet[T]): ExpressionOperation[JavaBatchTranslator] = {
    new JavaBatchTranslator().createExpressionOperation(set)
  }

  /**
   * Transforms the given DataStream to a [[org.apache.flink.api.expressions.ExpressionOperation]].
   * The fields of the DataSet type are renamed to the given set of fields:
   *
   * Example:
   *
   * {{{
   *   ExpressionUtil.from(set, "a, b")
   * }}}
   *
   * This will transform the set containing elements of two fields to a table where the fields
   * are named a and b.
   */
  def from[T](set: DataStream[T], fields: String): ExpressionOperation[JavaStreamingTranslator] = {
    new JavaStreamingTranslator().createExpressionOperation(set, fields)
  }

  /**
   * Transforms the given DataSet to a [[org.apache.flink.api.expressions.ExpressionOperation]].
   * The fields of the DataSet type are used to name the
   * [[org.apache.flink.api.expressions.ExpressionOperation]] fields.
   */
  def from[T](set: DataStream[T]): ExpressionOperation[JavaStreamingTranslator] = {
    new JavaStreamingTranslator().createExpressionOperation(set)
  }

  /**
   * Converts the given [[org.apache.flink.api.expressions.ExpressionOperation]] to
   * a DataSet. The given type must have exactly the same fields as the
   * [[org.apache.flink.api.expressions.ExpressionOperation]]. That is, the names of the
   * fields and the types must match.
   */
  @SuppressWarnings(Array("unchecked"))
  def toSet[T](
      op: ExpressionOperation[JavaBatchTranslator],
      clazz: Class[T]): DataSet[T] = {
    op.as(TypeExtractor.createTypeInfo(clazz)).asInstanceOf[DataSet[T]]
  }

  /**
   * Converts the given [[org.apache.flink.api.expressions.ExpressionOperation]] to
   * a DataStream. The given type must have exactly the same fields as the
   * [[org.apache.flink.api.expressions.ExpressionOperation]]. That is, the names of the
   * fields and the types must match.
   */
  @SuppressWarnings(Array("unchecked"))
  def toStream[T](
      op: ExpressionOperation[JavaStreamingTranslator], clazz: Class[T]): DataStream[T] = {
    op.as(TypeExtractor.createTypeInfo(clazz)).asInstanceOf[DataStream[T]]
  }
}

