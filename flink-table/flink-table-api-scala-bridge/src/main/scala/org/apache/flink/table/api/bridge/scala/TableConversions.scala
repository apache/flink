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

package org.apache.flink.table.api.bridge.scala

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.internal.TableImpl
import org.apache.flink.table.api.{Schema, Table, TableException, ValidationException}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.types.{AbstractDataType, DataType}
import org.apache.flink.types.Row

/**
 * Holds methods to convert a [[Table]] into a [[DataStream]].
 *
 * @param table The [[Table]] to convert.
 */
@PublicEvolving
class TableConversions(table: Table) {

  private val internalEnv = table.asInstanceOf[TableImpl].getTableEnvironment

  /**
   * Converts the given [[Table]] into a [[DataStream]].
   *
   * The [[Table]] to convert must be insert-only.
   *
   * See [[StreamTableEnvironment.toDataStream(Table)]] for more information.
   *
   * @return The converted [[DataStream]].
   */
  def toDataStream: DataStream[Row] = {
    internalEnv match {
      case tableEnv: StreamTableEnvironment =>
        tableEnv.toDataStream(table)
      case _ =>
        throw new ValidationException(
          "Table cannot be converted into a Scala DataStream. " +
          "It is not part of a Scala StreamTableEnvironment.")
    }
  }

  /**
   * Converts the given [[Table]] into a [[DataStream]].
   *
   * The [[Table]] to convert must be insert-only.
   *
   * See [[StreamTableEnvironment.toDataStream(Table, Class)]] for more information.
   *
   * @param targetClass The [[Class]] that decides about the final external representation in
   *                    [[DataStream]] records.
   * @return The converted [[DataStream]].
   */
  def toDataStream[T](targetClass: Class[T]): DataStream[T] = {
    internalEnv match {
      case tableEnv: StreamTableEnvironment =>
        tableEnv.toDataStream(table, targetClass)
      case _ =>
        throw new ValidationException(
          "Table cannot be converted into a Scala DataStream. " +
          "It is not part of a Scala StreamTableEnvironment.")
    }
  }

  /**
   * Converts the given [[Table]] into a [[DataStream]].
   *
   * The [[Table]] to convert must be insert-only.
   *
   * See [[StreamTableEnvironment.toDataStream(Table, AbstractDataType)]] for more information.
   *
   * @param targetDataType The [[DataType]] that decides about the final external
   *                       representation in [[DataStream]] records.
   * @return The converted [[DataStream]].
   */
  def toDataStream[T](targetDataType: AbstractDataType[_]): DataStream[T] = {
    internalEnv match {
      case tableEnv: StreamTableEnvironment =>
        tableEnv.toDataStream(table, targetDataType)
      case _ =>
        throw new ValidationException(
          "Table cannot be converted into a Scala DataStream. " +
          "It is not part of a Scala StreamTableEnvironment.")
    }
  }

  /**
   * Converts the given [[Table]] into a [[DataStream]] of changelog entries.
   *
   * The [[Table]] to convert can be updating or insert-only.
   *
   * See [[StreamTableEnvironment.toChangelogStream(Table)]] for more information.
   *
   * @return The converted changelog stream of [[Row]].
   */
  def toChangelogStream: DataStream[Row] = {
    internalEnv match {
      case tableEnv: StreamTableEnvironment =>
        tableEnv.toChangelogStream(table)
      case _ =>
        throw new ValidationException(
          "Table cannot be converted into a Scala DataStream. " +
          "It is not part of a Scala StreamTableEnvironment.")
    }
  }

  /**
   * Converts the given [[Table]] into a [[DataStream]] of changelog entries.
   *
   * The [[Table]] to convert can be updating or insert-only.
   *
   * See [[StreamTableEnvironment.toChangelogStream(Table, Schema)]] for more information.
   *
   * @param targetSchema The [[Schema]] that decides about the final external representation
   *                     in [[DataStream]] records.
   * @return The converted changelog stream of [[Row]].
   */
  def toChangelogStream(targetSchema: Schema): DataStream[Row] = {
    internalEnv match {
      case tableEnv: StreamTableEnvironment =>
        tableEnv.toChangelogStream(table, targetSchema)
      case _ =>
        throw new ValidationException(
          "Table cannot be converted into a Scala DataStream. " +
          "It is not part of a Scala StreamTableEnvironment.")
    }
  }

  /**
   * Converts the given [[Table]] into a [[DataStream]] of changelog entries.
   *
   * The [[Table]] to convert can be updating or insert-only.
   *
   * See [[StreamTableEnvironment.toChangelogStream(Table, Schema, ChangelogMode)]] for more
   * information.
   *
   * @param targetSchema The [[Schema]] that decides about the final external representation
   *                     in [[DataStream]] records.
   * @param changelogMode The required kinds of changes in the result changelog. An exception will
   *                      be thrown if the given updating table cannot be represented in this
   *                      changelog mode.
   * @return The converted changelog stream of [[Row]].
   */
  def toChangelogStream(targetSchema: Schema, changelogMode: ChangelogMode): DataStream[Row] = {
    internalEnv match {
      case tableEnv: StreamTableEnvironment =>
        tableEnv.toChangelogStream(table, targetSchema, changelogMode)
      case _ =>
        throw new ValidationException(
          "Table cannot be converted into a Scala DataStream. " +
          "It is not part of a Scala StreamTableEnvironment.")
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Legacy before FLIP-136
  // ----------------------------------------------------------------------------------------------

  /**
    * Converts the given [[Table]] into an append [[DataStream]] of a specified type.
    *
    * The [[Table]] must only have insert (append) changes. If the [[Table]] is also modified
    * by update or delete changes, the conversion will fail.
    *
    * The fields of the [[Table]] are mapped to [[DataStream]] fields as follows:
    * - [[org.apache.flink.types.Row]] and Scala Tuple types: Fields are mapped by position, field
    * types must match.
    * - POJO [[DataStream]] types: Fields are mapped by field name, field types must match.
    *
    * @tparam T The type of the resulting [[DataStream]].
    * @return The converted [[DataStream]].
    */
  def toAppendStream[T: TypeInformation]: DataStream[T] = {
    internalEnv match {
      case tEnv: StreamTableEnvironment =>
        tEnv.toAppendStream(table)
      case _ =>
        throw new ValidationException(
          "Only tables that originate from Scala DataStreams " +
            "can be converted to Scala DataStreams.")
    }
  }

  /** Converts the [[Table]] to a [[DataStream]] of add and retract messages.
    * The message will be encoded as [[Tuple2]]. The first field is a [[Boolean]] flag,
    * the second field holds the record of the specified type [[T]].
    *
    * A true [[Boolean]] flag indicates an add message, a false flag indicates a retract message.
    *
    */
  def toRetractStream[T: TypeInformation]: DataStream[(Boolean, T)] = {
    internalEnv match {
      case tEnv: StreamTableEnvironment =>
        tEnv.toRetractStream(table)
      case _ =>
        throw new TableException(
          "Only tables that originate from Scala DataStreams " +
            "can be converted to Scala DataStreams.")
    }
  }
}

