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

package org.apache.flink.tests.util.kafka.hybrid;

import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

import static org.apache.flink.tests.util.kafka.hybrid.TestDataUtils.divide;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class HybridDataWriter implements SourceSplitDataWriter<String> {

    private static final Logger LOG = LoggerFactory.getLogger(HybridDataWriter.class);
    public static final double defaultDivideFraction = 0.5;

    private final SourceSplitDataWriter<String> fileDataWriter;
    private final SourceSplitDataWriter<String> kafkaDataWriter;

    public HybridDataWriter(
            SourceSplitDataWriter<String> fileDataWriter,
            SourceSplitDataWriter<String> kafkaDataWriter) {
        checkNotNull(fileDataWriter);
        checkNotNull(kafkaDataWriter);

        this.fileDataWriter = fileDataWriter;
        this.kafkaDataWriter = kafkaDataWriter;
    }

    @Override
    public void writeRecords(Collection<String> records) {
        // Implicitly split the records between the two sources.
        List<Collection<String>> parts = divide(records, defaultDivideFraction);
        fileDataWriter.writeRecords(parts.get(0));
        kafkaDataWriter.writeRecords(parts.get(1));
    }

    @Override
    public void close() throws Exception {
        fileDataWriter.close();
        kafkaDataWriter.close();
    }
}
