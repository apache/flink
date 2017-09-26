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

/**
  * Defines a logical event-time attribute for a [[TableSource]].
  * The event-time attribute can be used for indicating, accessing, and working with Flink's
  * event-time.
  *
  * A [[TableSource]] that implements this interface defines the name of
  * the event-time attribute. The attribute must be present in the schema of the [[TableSource]]
  * and must be of type [[Long]] or [[java.sql.Timestamp]].
  */
trait DefinedRowtimeAttribute {

  /**
    * Defines a name of the event-time attribute that represents Flink's event-time, i.e., an
    * attribute that is aligned with the watermarks of the
    * [[org.apache.flink.streaming.api.datastream.DataStream]] returned by
    * [[StreamTableSource.getDataStream()]].
    *
    * An attribute with the given name must be present in the schema of the [[TableSource]].
    * The attribute must be of type [[Long]] or [[java.sql.Timestamp]].
    *
    * The method should return null if no rowtime attribute is defined.
    *
    * @return The name of the field that represents the event-time field and which is aligned
    *         with the watermarks of the [[org.apache.flink.streaming.api.datastream.DataStream]]
    *         returned by [[StreamTableSource.getDataStream()]].
    *         The field must be present in the schema of the [[TableSource]] and be of type [[Long]]
    *         or [[java.sql.Timestamp]].
    */
  def getRowtimeAttribute: String
}

/**
  * Defines a logical processing-time attribute for a [[TableSource]].
  * The processing-time attribute can be used for indicating, accessing, and working with Flink's
  * processing-time.
  *
  * A [[TableSource]] that implements this interface defines the name of
  * the processing-time attribute. The attribute will be added to the schema of the
  * [[org.apache.flink.table.api.Table]] produced by the [[TableSource]].
  */
trait DefinedProctimeAttribute {

  /**
    * Defines a name of the processing-time attribute that represents Flink's
    * processing-time. Null if no rowtime should be available.
    *
    * The field will be appended to the schema provided by the [[TableSource]].
    */
  def getProctimeAttribute: String

}
