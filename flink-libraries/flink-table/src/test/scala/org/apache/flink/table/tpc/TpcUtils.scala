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

package org.apache.flink.table.tpc

import java.io.File
import java.text.DecimalFormat
import java.util.{ArrayList => JArrayList, List => JList}
import java.math.{BigDecimal => JBigDecimal}
import java.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfigOptions}
import org.apache.flink.table.api.types.InternalType
import org.apache.flink.types.Row

import scala.collection.JavaConversions._

object TpcUtils {

  def getTpcHQuery(caseName: String): String = {
    TpcUtils.resourceToString(s"/tpch/queries/$caseName.sql")
  }

  def getStreamTpcHQuery(caseName: String): String = {
    TpcUtils.resourceToString(s"/tpch/queries-stream/$caseName.sql")
  }

  def getTpcHResult(caseName: String): String = {
    resourceToString(s"/tpch/result/${caseName.replace("_1", "")}.out")
  }

  def getStreamTpcHResult(caseName: String): String = {
    resourceToString(s"/tpch/result-stream/${caseName.replace("_1", "")}.out")
  }

  def getTpcDsQuery(caseName: String, factor: Int): String = {
    TpcUtils.resourceToString(s"/tpcds/queries/$factor/$caseName.sql")
  }

  def getStreamTpcDsQuery(caseName: String): String = {
    TpcUtils.resourceToString(s"/tpcds/queries-stream/$caseName.sql")
  }

  def resourceToString(resource: String): String = {
    scala.io.Source.fromFile(new File(getClass.getResource(resource).getFile)).mkString
  }

  def disableBroadcastHashJoin(tEnv: BatchTableEnvironment): Unit = {
    val config = new Configuration()
    config.addAll(tEnv.getConfig.getConf)
    config.setLong(TableConfigOptions.SQL_EXEC_HASH_JOIN_BROADCAST_THRESHOLD, -1)
    tEnv.getConfig.setConf(config)
  }

  def disableRangeSort(tEnv: BatchTableEnvironment): Unit = {
    val config = new Configuration()
    config.addAll(tEnv.getConfig.getConf)
    config.setBoolean(TableConfigOptions.SQL_EXEC_SORT_RANGE_ENABLED, false)
    tEnv.getConfig.setConf(config)
  }

  def disableParquetFilterPushDown(tEnv: BatchTableEnvironment): Unit = {
    val config = new Configuration()
    config.addAll(tEnv.getConfig.getConf)
    config.setBoolean(TableConfigOptions.SQL_PARQUET_PREDICATE_PUSHDOWN_ENABLED, false)
    config.getBoolean(TableConfigOptions.SQL_ORC_PREDICATE_PUSHDOWN_ENABLED, false)
    tEnv.getConfig.setConf(config)
  }

  def formatResult(result: Seq[Row]): JList[String] = {
    result.map((row) => {
      val list = new JArrayList[Any]()
      for (i <- 0 until row.getArity) {
        val v = row.getField(i)
        val newV = v match {
          case b: JBigDecimal => new DecimalFormat("0.0000").format(b)
          case d: java.lang.Double => new DecimalFormat("0.0000").format(d)
          case _ => v
        }
        list.add(newV)
      }
      list.toString
    })
  }

}

trait Schema {

  def getFieldNames: Array[String]

  def getFieldTypes: Array[InternalType]

  def getFieldNullables: Array[Boolean]

  def getUniqueKeys: util.Set[util.Set[String]] = null

}

trait TpchSchema extends Schema {

  /**
    * Each column is not nullable for each tpch table based on TPCH Documents.
    */
  override def getFieldNullables: Array[Boolean] = getFieldNames.map(f => false)

}

object STATS_MODE extends Enumeration {
  type STATS_MODE = Value
  val FULL, PART, ROW_COUNT = Value
}
