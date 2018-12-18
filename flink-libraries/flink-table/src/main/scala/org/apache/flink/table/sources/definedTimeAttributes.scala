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

package org.apache.flink.table.sources

import java.util
import java.util.Objects
import javax.annotation.Nullable

import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.Types
import org.apache.flink.table.sources.tsextractors.TimestampExtractor
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy

/**
  * Extends a [[TableSource]] to specify a processing time attribute.
  */
trait DefinedProctimeAttribute {

  /**
    * Returns the name of a processing time attribute or null if no processing time attribute is
    * present.
    *
    * The referenced attribute must be present in the [[TableSchema]] of the [[TableSource]] and of
    * type [[Types.SQL_TIMESTAMP]].
    */
  @Nullable
  def getProctimeAttribute: String
}

/**
  * Extends a [[TableSource]] to specify rowtime attributes via a
  * [[RowtimeAttributeDescriptor]].
  */
trait DefinedRowtimeAttributes {

  /**
    * Returns a list of [[RowtimeAttributeDescriptor]] for all rowtime attributes of the table.
    *
    * All referenced attributes must be present in the [[TableSchema]] of the [[TableSource]] and of
    * type [[Types.SQL_TIMESTAMP]].
    *
    * @return A list of [[RowtimeAttributeDescriptor]].
    */
  def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor]
}

/**
  * Describes a rowtime attribute of a [[TableSource]].
  *
  * @param attributeName The name of the rowtime attribute.
  * @param timestampExtractor The timestamp extractor to derive the values of the attribute.
  * @param watermarkStrategy The watermark strategy associated with the attribute.
  */
class RowtimeAttributeDescriptor(
  val attributeName: String,
  val timestampExtractor: TimestampExtractor,
  val watermarkStrategy: WatermarkStrategy) {

  /** Returns the name of the rowtime attribute. */
  def getAttributeName: String = attributeName

  /** Returns the [[TimestampExtractor]] for the attribute. */
  def getTimestampExtractor: TimestampExtractor = timestampExtractor

  /** Returns the [[WatermarkStrategy]] for the attribute. */
  def getWatermarkStrategy: WatermarkStrategy = watermarkStrategy

  override def equals(other: Any): Boolean = other match {
    case that: RowtimeAttributeDescriptor =>
        Objects.equals(attributeName, that.attributeName) &&
        Objects.equals(timestampExtractor, that.timestampExtractor) &&
        Objects.equals(watermarkStrategy, that.watermarkStrategy)
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hash(attributeName, timestampExtractor, watermarkStrategy)
  }
}
