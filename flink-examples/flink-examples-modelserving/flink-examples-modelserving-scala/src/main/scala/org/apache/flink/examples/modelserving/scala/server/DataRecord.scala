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

package org.apache.flink.examples.modelserving.scala.server

import org.apache.flink.modelserving.wine.winerecord.WineRecord
import org.apache.flink.modelserving.scala.model.DataToServe
import scala.util.Try

/**
  * Implementation of Container for wine data.
  */
object DataRecord {

  /**
    * Convert data record.
    *
    * @param message byte message
    * @return data record.
    */
  def fromByteArray(message: Array[Byte]): Try[DataToServe[WineRecord]] = Try {
    DataRecord(WineRecord.parseFrom(message))
  }
}

case class DataRecord(record : WineRecord) extends DataToServe[WineRecord]{
  /**
    * Get type.
    *
    * @return data type.
    */
  def getType : String = record.dataType

  /**
    * Get record.
    *
    * @return data record.
    */
  def getRecord : WineRecord = record
}
