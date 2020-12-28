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

package org.apache.flink.orc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** InputFormat to read ORC files into {@link Row}. */
public class OrcRowInputFormat extends OrcInputFormat<Row> implements ResultTypeQueryable<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(OrcRowInputFormat.class);

    // the number of rows read in a batch
    private static final int DEFAULT_BATCH_SIZE = 1000;

    // the type information of the Rows returned by this InputFormat.
    private transient RowTypeInfo rowType;

    /**
     * Creates an OrcRowInputFormat.
     *
     * @param path The path to read ORC files from.
     * @param schemaString The schema of the ORC files as String.
     * @param orcConfig The configuration to read the ORC files with.
     */
    public OrcRowInputFormat(String path, String schemaString, Configuration orcConfig) {
        this(path, TypeDescription.fromString(schemaString), orcConfig, DEFAULT_BATCH_SIZE);
    }

    /**
     * Creates an OrcRowInputFormat.
     *
     * @param path The path to read ORC files from.
     * @param schemaString The schema of the ORC files as String.
     * @param orcConfig The configuration to read the ORC files with.
     * @param batchSize The number of Row objects to read in a batch.
     */
    public OrcRowInputFormat(
            String path, String schemaString, Configuration orcConfig, int batchSize) {
        this(path, TypeDescription.fromString(schemaString), orcConfig, batchSize);
    }

    /**
     * Creates an OrcRowInputFormat.
     *
     * @param path The path to read ORC files from.
     * @param orcSchema The schema of the ORC files as ORC TypeDescription.
     * @param orcConfig The configuration to read the ORC files with.
     * @param batchSize The number of Row objects to read in a batch.
     */
    public OrcRowInputFormat(
            String path, TypeDescription orcSchema, Configuration orcConfig, int batchSize) {
        super(new Path(path), orcSchema, orcConfig, batchSize);
        this.rowType = (RowTypeInfo) OrcBatchReader.schemaToTypeInfo(orcSchema);
    }

    @Override
    public void selectFields(int... selectedFields) {
        super.selectFields(selectedFields);
        // adapt result type
        this.rowType = RowTypeInfo.projectFields(this.rowType, selectedFields);
    }

    @Override
    public void open(FileInputSplit fileSplit) throws IOException {
        LOG.debug("Opening ORC file {}", fileSplit.getPath());
        this.reader =
                new OrcRowSplitReader(
                        conf,
                        schema,
                        selectedFields,
                        conjunctPredicates,
                        batchSize,
                        fileSplit.getPath(),
                        fileSplit.getStart(),
                        fileSplit.getLength());
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return rowType;
    }
}
