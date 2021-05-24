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

package org.apache.flink.formats.parquet;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.BulkWriter;

import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A simple {@link BulkWriter} implementation that wraps a {@link ParquetWriter}.
 *
 * @param <T> The type of records written.
 */
@PublicEvolving
public class ParquetBulkWriter<T> implements BulkWriter<T> {

    /** The ParquetWriter to write to. */
    private final ParquetWriter<T> parquetWriter;

    /**
     * Creates a new ParquetBulkWriter wrapping the given ParquetWriter.
     *
     * @param parquetWriter The ParquetWriter to write to.
     */
    public ParquetBulkWriter(ParquetWriter<T> parquetWriter) {
        this.parquetWriter = checkNotNull(parquetWriter, "parquetWriter");
    }

    @Override
    public void addElement(T datum) throws IOException {
        parquetWriter.write(datum);
    }

    @Override
    public void flush() {
        // nothing we can do here
    }

    @Override
    public void finish() throws IOException {
        parquetWriter.close();
    }
}
