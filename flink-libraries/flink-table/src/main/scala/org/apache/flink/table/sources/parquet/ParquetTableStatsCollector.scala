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

package org.apache.flink.table.sources.parquet

import java.io.IOException
import java.lang.Long
import java.math.BigDecimal
import java.sql.Timestamp
import java.util
import java.util.concurrent.{Callable, Executors, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.calcite.avatica.util.DateTimeUtils
import org.apache.flink.core.fs.{FileStatus, FileSystem, Path}
import org.apache.flink.runtime.util.Hardware
import org.apache.flink.table.api.types.{DataTypes, DecimalType, InternalType}
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.runtime.functions.BuildInScalarFunctions.{internalToDate, internalToTime}
import org.apache.flink.table.util.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.column.statistics._
import org.apache.parquet.example.data.simple.NanoTime
import org.apache.parquet.filter2.compat.{FilterCompat, RowGroupFilter}
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader

import scala.collection.JavaConversions._

object ParquetTableStatsCollector extends Logging {
  // default timeout: 60 Seconds
  val defaultTimeout = 60000
  val timeoutTimeUnit = TimeUnit.MILLISECONDS

  /**
    * Get statistics from specific Parquet path, including rowCount, nullCount, max and min.
    *
    * @param filePath The path to the parquet file.
    * @param fieldTypes The types of the table fields.
    * @param fieldNames The names of the table fields.
    * @param nestedFileEnumeration The flag to specify whether recursive traversal
    *                              of the input directory structure is enabled.
    * @param filter A [[FilterPredicate]] applies it to a list of BlockMetaData to get row groups
    *               matched this filter.
    * @param hadoopConf Hadoop configuration to use while reading Parquet Footer.
    *                   e.g. config item: parquet.strings.signed-min-max.enabled
    * @param maxThreads The maximum number of threads for concurrently reading Parquet metadata.
    * @return [[TableStats]] represents Parquet [[Statistics]] in specific path.
    */
  def collectTableStats(
    filePath: Path,
    nestedFileEnumeration: Boolean,
    fieldNames: Array[String],
    fieldTypes: Array[InternalType],
    filter: Option[FilterPredicate] = None,
    hadoopConf: Option[Configuration] = None,
    maxThreads: Option[Int] = None): TableStats = {
    val startTime = System.currentTimeMillis()

    val fileStatus = listFileStatus(filePath, nestedFileEnumeration, fieldNames, fieldTypes)
    if (fileStatus.isEmpty) {
      return TableStats(0L)
    }

    val config = hadoopConf.getOrElse(new Configuration())
    val (totalRowCount, finalStatsMap) = collectStatistics(
      fileStatus, fieldNames, config, filter, maxThreads)

    val finalColumnStats = finalStatsMap.map {
      case (name, stats) =>
        val index = fieldNames.indexOf(name)
        val typeInfo = fieldTypes(index)
        val columnStats = new ColumnStats(
          ndv = null,
          nullCount = if (stats.isEmpty) null else stats.getNumNulls,
          avgLen = null,
          maxLen = null,
          max = getMaxMinValueByTypeInfo(typeInfo, stats, isMax = true),
          min = getMaxMinValueByTypeInfo(typeInfo, stats, isMax = false)
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
    filter: Option[FilterPredicate],
    maxThreads: Option[Int]): (Long, util.Map[String, Statistics[_]]) = {
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
      .setNameFormat("Collect Parquet statistics Thread %d")
      .build()
    val executor = Executors.newFixedThreadPool(numOfThreads, threadFactory)

    var totalRowCount: Long = 0L
    val finalStatsMap = new util.HashMap[String, Statistics[_]]()
    try {
      val results = fileStatus.map { file =>
        executor.submit(new Callable[(Long, util.Map[String, Statistics[_]])] {
          override def call(): (Long, util.Map[String, Statistics[_]]) = {
            getStatisticsOfFile(file, fieldNames, hadoopConf, filter)
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
  private def getMaxMinValueByTypeInfo(
    typeInfo: InternalType,
    stat: Statistics[_],
    isMax: Boolean): Any = {
    typeInfo match {
      case DataTypes.BOOLEAN => stat match {
        case s: BooleanStatistics => if (isMax) s.getMax else s.getMin
        case _ => null
      }
      case DataTypes.FLOAT => stat match {
        case s: FloatStatistics => if (isMax) s.getMax else s.getMin
        case _ => null
      }
      case DataTypes.INT => stat match {
        case s: IntStatistics => if (isMax) s.getMax else s.getMin
        case _ => null
      }
      case DataTypes.LONG => stat match {
        case s: LongStatistics => if (isMax) s.getMax else s.getMin
        case _ => null
      }
      case DataTypes.DOUBLE => stat match {
        case s: DoubleStatistics => if (isMax) s.getMax else s.getMin
        case _ => null
      }
      case _: DecimalType =>
        stat match {
          case s: FloatStatistics =>
            BigDecimal.valueOf(if (isMax) s.getMax else s.getMin)
          case s: IntStatistics =>
            BigDecimal.valueOf(if (isMax) s.getMax else s.getMin)
          case s: LongStatistics =>
            BigDecimal.valueOf(if (isMax) s.getMax else s.getMin)
          case s: DoubleStatistics =>
            BigDecimal.valueOf(if (isMax) s.getMax else s.getMin)
          case _ => null
        }
      case DataTypes.STRING => stat match {
        case s: BinaryStatistics =>
          val v = if (isMax) s.genericGetMax else s.genericGetMin
          if (v != null) {
            v.toStringUsingUTF8
          } else {
            null
          }
        case _ => null
      }
      case DataTypes.DATE => stat match {
        case s: IntStatistics =>
          val v = if (isMax) s.getMax else s.getMin
          internalToDate(v)
        case _ => null
      }
      case DataTypes.TIME => stat match {
        case s: IntStatistics =>
          val v = if (isMax) s.getMax else s.getMin
          internalToTime(v)
        case _ => null
      }
      case DataTypes.TIMESTAMP => stat match {
        case s: LongStatistics => new Timestamp(if (isMax) s.getMax else s.getMin)
        case s: BinaryStatistics =>
          val v = if (isMax) s.genericGetMax else s.genericGetMin
          if (v != null) {
            val nt = NanoTime.fromBinary(v)
            val ts = DateTimeUtils.julianDayToTimestamp(nt.getJulianDay, nt.getTimeOfDayNanos)
            new Timestamp(ts)
          } else {
            null
          }
        case _ => null
      }
      case _ => null
    }
  }

  /**
    * Get total rowCount and merged Statistics for each column in specific file.
    */
  private def getStatisticsOfFile(
    fileStatus: FileStatus,
    fieldNames: Array[String],
    hadoopConf: Configuration,
    filter: Option[FilterPredicate]): (Long, util.Map[String, Statistics[_]]) = {
    val metadata = ParquetFileReader.readFooter(
      hadoopConf,
      new org.apache.hadoop.fs.Path(fileStatus.getPath.toUri),
      ParquetMetadataConverter.NO_FILTER)
    val schema = metadata.getFileMetaData.getSchema
    val nameToIndexMap = schema.asGroupType().getFields.map(_.getName).zipWithIndex.toMap

    var rowCount: Long = 0L
    val statsMap = new util.HashMap[String, Statistics[_]]()

    val originBlocks = metadata.getBlocks
    val filteredBlocks = filter match {
      case Some(f) => RowGroupFilter.filterRowGroups(FilterCompat.get(f), originBlocks, schema)
      case _ => originBlocks
    }
    filteredBlocks.foreach { b =>
      rowCount += b.getRowCount
      fieldNames.foreach { n =>
        val i = nameToIndexMap.getOrElse(n, -1)
        if (i >= 0) {
          updateStatistics(b.getColumns.get(i).getStatistics, n, statsMap)
        }
      }
    }
    (rowCount, statsMap)
  }

  private def updateStatistics(
    newStats: Statistics[_],
    fieldName: String,
    statsMap: util.Map[String, Statistics[_]]): Unit = {
    val oldStats = statsMap.get(fieldName)
    if (oldStats != null) {
      newStats match {
        case s: BooleanStatistics => oldStats.asInstanceOf[BooleanStatistics].mergeStatistics(s)
        case s: FloatStatistics => oldStats.asInstanceOf[FloatStatistics].mergeStatistics(s)
        case s: IntStatistics => oldStats.asInstanceOf[IntStatistics].mergeStatistics(s)
        case s: LongStatistics => oldStats.asInstanceOf[LongStatistics].mergeStatistics(s)
        case s: DoubleStatistics => oldStats.asInstanceOf[DoubleStatistics].mergeStatistics(s)
        case s: BinaryStatistics => oldStats.asInstanceOf[BinaryStatistics].mergeStatistics(s)
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

    // create a ParquetInputFormat for reusing `acceptFile` method
    val inputFormat = new ParquetInputFormat[Any, Any](filePath, fieldTypes, fieldNames) {
      override def convert(r: Any): AnyRef = ???

      @throws[IOException]
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
