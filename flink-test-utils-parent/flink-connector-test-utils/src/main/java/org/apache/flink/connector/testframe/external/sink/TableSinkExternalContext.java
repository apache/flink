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

package org.apache.flink.connector.testframe.external.sink;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.connector.testframe.external.ExternalContext;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.Map;

/**
 * External context for table sinks.
 *
 * <p>Comparing with {@link DataStreamSinkExternalContext}, the data type of this external context
 * is fixed as {@link RowData} to test functionality of table source.
 */
@Experimental
public interface TableSinkExternalContext extends ExternalContext {

    /** Get table options for building DDL of the connector sink table. */
    Map<String, String> getSinkTableOptions(TestingSinkSettings sinkSettings)
            throws UnsupportedOperationException;

    /**
     * Create a new split in the external system and return a data writer corresponding to the new
     * split.
     */
    ExternalSystemDataReader<RowData> createSinkRowDataReader(
            TestingSinkSettings sinkOptions, DataType dataType);
}
