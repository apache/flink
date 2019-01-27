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

package org.apache.flink.table.sinks.parquet

import java.io.File
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api.{TableConfig, TableConfigOptions}
import org.apache.flink.table.api.types.{DataType, RowType}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.sinks.{BatchTableSink, TableSinkBase}

import org.apache.hadoop.fs.FileUtil
import org.apache.parquet.hadoop.metadata.CompressionCodecName

/**
  * A subclass of [[BatchTableSink]] to write [[BaseRow]] to Parquet files.
  *
  * @param dir         The output path to write the Table to.
  * @param writeMode   The write mode to specify whether existing files are overwritten or not.
  * @param compression compression algorithms.
  */
class ParquetTableSink(
    dir: String,
    writeMode: Option[WriteMode] = None,
    compression: CompressionCodecName = CompressionCodecName.UNCOMPRESSED)
    extends TableSinkBase[BaseRow]
    with BatchTableSink[BaseRow]{

  /** Return a deep copy of the [[org.apache.flink.table.sinks.TableSink]]. */
  override protected def copy: TableSinkBase[BaseRow] = {
    new ParquetTableSink(dir, writeMode, compression)
  }

  override def getOutputType: DataType =
    new RowType(getFieldTypes: _*)

  /** Emits the BoundedStream. */
  override def emitBoundedStream(boundedStream: DataStream[BaseRow],
                                  tableConfig: TableConfig,
                                  executionConfig: ExecutionConfig): DataStreamSink[BaseRow] = {
    writeMode match {
      case Some(wm) if wm == WriteMode.OVERWRITE =>
        FileUtil.fullyDelete(new File(dir))
      case _ =>
        val path = new Path(dir)
        if (path.getFileSystem.exists(path) && !path.getFileSystem.getFileStatus(path).isDir) {
          throw new RuntimeException( "output dir [" + dir + "] already existed.")
        }
    }

    val blockSize =
      tableConfig.getConf.getInteger(
        TableConfigOptions.SQL_PARQUET_SINK_BLOCK_SIZE)

    val enableDictionary =
      tableConfig.getConf.getBoolean(
        TableConfigOptions.SQL_PARQUET_SINK_DICTIONARY_ENABLED)

    boundedStream.writeUsingOutputFormat(new RowParquetOutputFormat(
      dir, getFieldTypes.map(_.toInternalType), getFieldNames, compression,
      blockSize, enableDictionary)).name("parquet sink: " + dir)
  }
}
