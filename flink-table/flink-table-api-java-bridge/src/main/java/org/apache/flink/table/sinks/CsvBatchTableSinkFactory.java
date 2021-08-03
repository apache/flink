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

package org.apache.flink.table.sinks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.types.Row;

import java.util.Map;

/**
 * Factory base for creating configured instances of {@link CsvTableSink} in a batch environment.
 *
 * @deprecated The legacy CSV connector has been replaced by {@link FileSink}. It is kept only to
 *     support tests for the legacy connector stack.
 */
@Internal
@Deprecated
public class CsvBatchTableSinkFactory extends CsvTableSinkFactoryBase
        implements StreamTableSinkFactory<Row> {

    @Override
    public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
        return createTableSink(false, properties);
    }
}
