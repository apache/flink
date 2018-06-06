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

package org.apache.flink.table.descriptors

import org.apache.flink.table.api.Types
import org.apache.flink.table.descriptors.RowtimeValidator.{normalizeTimestampExtractor, normalizeWatermarkStrategy}
import org.apache.flink.table.sources.tsextractors.{ExistingField, StreamRecordTimestamp, TimestampExtractor}
import org.apache.flink.table.sources.wmstrategies.{AscendingTimestamps, BoundedOutOfOrderTimestamps, PreserveWatermarks, WatermarkStrategy}

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * Rowtime descriptor for describing an event time attribute in the schema.
  */
class Rowtime extends Descriptor {

  private var timestampExtractor: Option[TimestampExtractor] = None
  private var watermarkStrategy: Option[WatermarkStrategy] = None

  /**
    * Sets a built-in timestamp extractor that converts an existing [[Long]] or
    * [[Types.SQL_TIMESTAMP]] field into the rowtime attribute.
    *
    * @param fieldName The field to convert into a rowtime attribute.
    */
  def timestampsFromField(fieldName: String): Rowtime = {
    timestampExtractor = Some(new ExistingField(fieldName))
    this
  }

  /**
    * Sets a built-in timestamp extractor that converts the assigned timestamps from
    * a DataStream API record into the rowtime attribute and thus preserves the assigned
    * timestamps from the source.
    *
    * Note: This extractor only works in streaming environments.
    */
  def timestampsFromSource(): Rowtime = {
    timestampExtractor = Some(new StreamRecordTimestamp)
    this
  }

  /**
    * Sets a custom timestamp extractor to be used for the rowtime attribute.
    *
    * @param extractor The [[TimestampExtractor]] to extract the rowtime attribute
    *                  from the physical type.
    */
  def timestampsFromExtractor(extractor: TimestampExtractor): Rowtime = {
    timestampExtractor = Some(extractor)
    this
  }

  /**
    * Sets a built-in watermark strategy for ascending rowtime attributes.
    *
    * Emits a watermark of the maximum observed timestamp so far minus 1.
    * Rows that have a timestamp equal to the max timestamp are not late.
    */
  def watermarksPeriodicAscending(): Rowtime = {
    watermarkStrategy = Some(new AscendingTimestamps)
    this
  }

  /**
    * Sets a built-in watermark strategy for rowtime attributes which are out-of-order by a bounded
    * time interval.
    *
    * Emits watermarks which are the maximum observed timestamp minus the specified delay.
    *
    * @param delay delay in milliseconds
    */
  def watermarksPeriodicBounded(delay: Long): Rowtime = {
    watermarkStrategy = Some(new BoundedOutOfOrderTimestamps(delay))
    this
  }

  /**
    * Sets a built-in watermark strategy which indicates the watermarks should be preserved from the
    * underlying DataStream API and thus preserves the assigned watermarks from the source.
    */
  def watermarksFromSource(): Rowtime = {
    watermarkStrategy = Some(PreserveWatermarks.INSTANCE)
    this
  }

  /**
    * Sets a custom watermark strategy to be used for the rowtime attribute.
    */
  def watermarksFromStrategy(strategy: WatermarkStrategy): Rowtime = {
    watermarkStrategy = Some(strategy)
    this
  }

  /**
    * Internal method for properties conversion.
    */
  final override def addProperties(properties: DescriptorProperties): Unit = {
    val props = mutable.HashMap[String, String]()
    timestampExtractor.foreach(normalizeTimestampExtractor(_).foreach(e => props.put(e._1, e._2)))
    watermarkStrategy.foreach(normalizeWatermarkStrategy(_).foreach(e => props.put(e._1, e._2)))
    properties.putProperties(props.toMap.asJava)
  }
}

/**
  * Rowtime descriptor for describing an event time attribute in the schema.
  */
object Rowtime {

  /**
    * Rowtime descriptor for describing an event time attribute in the schema.
    */
  def apply(): Rowtime = new Rowtime()

}
