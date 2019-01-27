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

import java.util
import java.util.concurrent.{Callable, Executors, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.flink.core.fs.{FileStatus, FileSystem, Path}
import org.apache.flink.runtime.util.Hardware
import org.apache.flink.table.api.types.{DataTypes, DecimalType, InternalType}
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.util.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.orc.impl.ColumnStatisticsImpl
import org.apache.orc.{ColumnStatistics, DateColumnStatistics, DecimalColumnStatistics, DoubleColumnStatistics, IntegerColumnStatistics, OrcConf, OrcFile, StringColumnStatistics, TimestampColumnStatistics}

import scala.collection.JavaConversions._

object OrcTableStatsCollector extends Logging {
  // default timeout: 60 Seconds
  val defaultTimeout = 60000
  val timeoutTimeUnit = TimeUnit.MILLISECONDS

  /**
    * Get statistics from specific Orc path, including rowCount, nullCount, max and min.
    *
    * @param filePath The path to the orc file.
    * @param fieldTypes The types of the table fields.
    * @param fieldNames The names of the table fields.
    * @param nestedFileEnumeration The flag to specify whether recursive traversal
    *                              of the input directory structure is enabled.
    * @param hadoopConf Hadoop configuration to use while reading Orc Footer.
    * @param maxThreads The maximum number of threads for concurrently reading Orc metadata.
    * @return [[TableStats]] represents Orc Statistics in specific path.
    */
  def collectTableStats(
      filePath: Path,
      nestedFileEnumeration: Boolean,
      fieldNames: Array[String],
      fieldTypes: Array[InternalType],
      hadoopConf: Option[Configuration] = None,
      maxThreads: Option[Int] = None): TableStats = {
    val startTime = System.currentTimeMillis()

    val fileStatus = listFileStatus(filePath, nestedFileEnumeration, fieldNames, fieldTypes)
    if (fileStatus.isEmpty) {
      return TableStats(0L)
    }

    val config = hadoopConf.getOrElse(new Configuration())
    val (totalRowCount, finalStatsMap) = collectStatistics(
      fileStatus, fieldNames, config, maxThreads)

    val finalColumnStats = finalStatsMap.map {
      case (name, stats) =>
        val index = fieldNames.indexOf(name)
        val t = fieldTypes(index)
        val columnStats = new ColumnStats(
          ndv = null,
          nullCount = if (!stats.hasNull) null else totalRowCount - stats.getNumberOfValues,
          avgLen = null,
          maxLen = null,
          max = getMaxMinValueByType(t, stats, true),
          min = getMaxMinValueByType(t, stats, false)
        )
        (name, columnStats)
    }.toMap

    val endTime = System.currentTimeMillis()
    if (LOG.isDebugEnabled) {
      LOG.debug(s"collect TableStats from path: $filePath, cost: ${endTime - startTime}ms")
    }

    new TableStats(totalRowCount, finalColumnStats)
  }

  /**
    * Get statistics from specific files by multiple threads.
    */
  private def collectStatistics(
      fileStatus: util.List[FileStatus],
      fieldNames: Array[String],
      hadoopConf: Configuration,
      maxThreads: Option[Int]): (Long, util.Map[String, ColumnStatistics]) = {
    val numOfCpuCores = Hardware.getNumberCPUCores
    val defaultThreads = math.min(math.max(numOfCpuCores / 2, 1), fileStatus.size())
    val numOfThreads: Int = maxThreads match {
      case Some(t) if t > 0 =>
        val threads = math.min(math.min(numOfCpuCores, t), fileStatus.size())
        if (LOG.isDebugEnabled) {
          LOG.debug(s"max threads: $t, uses threads: $threads.")
        }
        threads
      case Some(t) =>
        LOG.warn(s"Illegal max threads: $t, uses default value: $defaultThreads.")
        defaultThreads
      case _ =>
        if (LOG.isDebugEnabled) {
          LOG.debug(s"Uses default threads: $defaultThreads.")
        }
        defaultThreads
    }

    val threadFactory = new ThreadFactoryBuilder()
      .setNameFormat("Collect Orc statistics Thread %d")
      .build()
    val executor = Executors.newFixedThreadPool(numOfThreads, threadFactory)

    var totalRowCount: Long = 0L
    val finalStatsMap = new util.HashMap[String, ColumnStatistics]()
    try {
      val results = fileStatus.map { file =>
        executor.submit(new Callable[(Long, util.Map[String, ColumnStatistics])] {
          override def call(): (Long, util.Map[String, ColumnStatistics]) = {
            getStatisticsOfFile(file, fieldNames, hadoopConf)
          }
        })
      }
      results.foreach { f =>
        val (rowCount, fileStatsMap) = f.get(defaultTimeout, timeoutTimeUnit)
        totalRowCount += rowCount
        fileStatsMap.foreach {
          case (fieldName, colStats) => updateStatistics(colStats, fieldName, finalStatsMap)
        }
      }
    } finally {
      executor.shutdownNow()
    }
    (totalRowCount, finalStatsMap)
  }

  /**
    * Get max/min value by expected InternalType
    */
  private def getMaxMinValueByType(
      t: InternalType,
      stat: ColumnStatistics,
      isMax: Boolean): Any = {
    t match {
      case DataTypes.BOOLEAN => null
      case DataTypes.BYTE | DataTypes.SHORT | DataTypes.INT | DataTypes.LONG =>
        stat match {
          case s: IntegerColumnStatistics => if (isMax) s.getMaximum else s.getMinimum
          case _ => null
        }
      case DataTypes.FLOAT | DataTypes.DOUBLE =>
        stat match {
          case s: DoubleColumnStatistics => if (isMax) s.getMaximum else s.getMinimum
          case _ => null
        }
      case DataTypes.STRING =>
        stat match {
          case s: StringColumnStatistics => if (isMax) s.getMaximum else s.getMinimum
          case _ => null
        }
      case _: DecimalType =>
        stat match {
          case s: DecimalColumnStatistics =>
            val v = if (isMax) s.getMaximum else s.getMinimum
            v.bigDecimalValue
          case _ => null
        }
      case DataTypes.DATE =>
        stat match {
          case s: DateColumnStatistics => if (isMax) s.getMaximum else s.getMinimum

          case _ => null
        }
      case DataTypes.TIMESTAMP =>
        stat match {
          case s: TimestampColumnStatistics => if (isMax) s.getMaximum else s.getMinimum
          case _ => null
        }
    }
  }

  /**
    * Get total rowCount and merged Statistics for each column in specific file.
    */
  private def getStatisticsOfFile(
      fileStatus: FileStatus,
      fieldNames: Array[String],
      hadoopConf: Configuration): (Long, util.Map[String, ColumnStatistics]) = {

    val filePath = new org.apache.hadoop.fs.Path(fileStatus.getPath.toUri)
    val reader = OrcFile.createReader(filePath,
      OrcFile.readerOptions(hadoopConf).maxLength(OrcConf.MAX_FILE_LENGTH.getLong(hadoopConf)))

    val rowCount = reader.getNumberOfRows
    val fileStats: Array[ColumnStatistics] = reader.getStatistics
    val schema = reader.getSchema
    val columnNames = schema.getFieldNames
    val columnTypes = schema.getChildren
    val fieldIndices = fieldNames.map(columnNames.indexOf(_))

    val statsMap = new util.HashMap[String, ColumnStatistics]()

    fieldNames.zip(fieldIndices).foreach {
      case (fieldName, fieldIdx) => {
        if (fieldIdx >= 0) {
          val colId = columnTypes.get(fieldIdx).getId
          statsMap.put(fieldName, fileStats.apply(colId))
        }
      }
    }

    (rowCount, statsMap)
  }

  private def updateStatistics(
      newStats: ColumnStatistics,
      fieldName: String,
      statsMap: util.Map[String, ColumnStatistics]): Unit = {
    val oldStats = statsMap.get(fieldName)
    if (oldStats != null) {
      newStats match {
        case s: ColumnStatisticsImpl => oldStats.asInstanceOf[ColumnStatisticsImpl].merge(s)
      }
    } else {
      statsMap.put(fieldName, newStats)
    }
  }

  /**
    * List all files for specific path.
    */
  private def listFileStatus(
      filePath: Path,
      nestedFileEnumeration: Boolean,
      fieldNames: Array[String],
      fieldTypes: Array[InternalType]): util.List[FileStatus] = {

    val inputFormat = new OrcInputFormat[Any, Any](filePath, fieldTypes, fieldNames) {

      override protected def convert(current: Any): AnyRef = ???

      def listFileStatus(path: Path, files: util.List[FileStatus]): Unit = {
        val fs: FileSystem = path.getFileSystem
        fs.listStatus(path).foreach { s =>
          if (s.isDir) {
            if (acceptFile(s) && nestedFileEnumeration) {
              listFileStatus(s.getPath, files)
            }
          } else {
            if (acceptFile(s)) {
              files.add(s)
            }
          }
        }
      }
    }

    val files = new util.ArrayList[FileStatus]()
    inputFormat.listFileStatus(filePath, files)
    files
  }
}
