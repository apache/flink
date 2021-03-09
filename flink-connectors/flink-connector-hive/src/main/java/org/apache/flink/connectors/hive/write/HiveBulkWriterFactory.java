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

package org.apache.flink.connectors.hive.write;

import org.apache.flink.formats.hadoop.bulk.HadoopPathBasedBulkWriter;
import org.apache.flink.table.data.RowData;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.function.Function;

/**
 * Hive bulk writer factory for path-based bulk file writer that writes to the specific hadoop path.
 */
public class HiveBulkWriterFactory implements HadoopPathBasedBulkWriter.Factory<RowData> {

    private static final long serialVersionUID = 1L;

    private final HiveWriterFactory factory;

    public HiveBulkWriterFactory(HiveWriterFactory factory) {
        this.factory = factory;
    }

    @Override
    public HadoopPathBasedBulkWriter<RowData> create(Path targetPath, Path inProgressPath)
            throws IOException {
        FileSinkOperator.RecordWriter recordWriter = factory.createRecordWriter(inProgressPath);
        Function<RowData, Writable> rowConverter = factory.createRowDataConverter();
        FileSystem fs = FileSystem.get(inProgressPath.toUri(), factory.getJobConf());
        return new HadoopPathBasedBulkWriter<RowData>() {

            @Override
            public long getSize() throws IOException {
                // it's possible the in-progress file hasn't yet been created, due to writer lazy
                // init or data buffering
                return fs.exists(inProgressPath) ? fs.getFileStatus(inProgressPath).getLen() : 0;
            }

            @Override
            public void dispose() {
                // close silently.
                try {
                    recordWriter.close(true);
                } catch (IOException ignored) {
                }
            }

            @Override
            public void addElement(RowData element) throws IOException {
                recordWriter.write(rowConverter.apply(element));
            }

            @Override
            public void flush() {}

            @Override
            public void finish() throws IOException {
                recordWriter.close(false);
            }
        };
    }
}
