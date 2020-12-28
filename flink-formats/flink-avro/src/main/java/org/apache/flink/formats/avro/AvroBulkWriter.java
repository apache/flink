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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.serialization.BulkWriter;

import org.apache.avro.file.DataFileWriter;

import java.io.IOException;

/** A simple {@link BulkWriter} implementation that wraps an Avro {@link DataFileWriter}. */
public class AvroBulkWriter<T> implements BulkWriter<T> {

    /** The underlying Avro writer. */
    private final DataFileWriter<T> dataFileWriter;

    /**
     * Create a new AvroBulkWriter wrapping the given Avro {@link DataFileWriter}.
     *
     * @param dataFileWriter The underlying Avro writer.
     */
    public AvroBulkWriter(DataFileWriter<T> dataFileWriter) {
        this.dataFileWriter = dataFileWriter;
    }

    @Override
    public void addElement(T element) throws IOException {
        dataFileWriter.append(element);
    }

    @Override
    public void flush() throws IOException {
        dataFileWriter.flush();
    }

    @Override
    public void finish() throws IOException {
        dataFileWriter.close();
    }
}
