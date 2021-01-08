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

package org.apache.flink.connectors.test.common.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

/** A simple file sink for writing records into a file locally. */
public class SimpleFileSink extends RichSinkFunction<String> {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleFileSink.class);
    String filePath;
    File sinkFile;
    BufferedWriter sinkBufferedWriter;
    boolean flushPerRecord;

    public SimpleFileSink(String filePath, boolean flushPerRecord) {
        this.filePath = filePath;
        this.flushPerRecord = flushPerRecord;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.sinkFile = new File(filePath);
        this.sinkBufferedWriter = new BufferedWriter(new FileWriter(sinkFile));
    }

    @Override
    public void close() throws Exception {
        LOG.debug("Closing SimpleFlinkSink...");
        sinkBufferedWriter.flush();
        sinkBufferedWriter.close();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        LOG.trace("Invoked with value: {}", value);
        sinkBufferedWriter.append(value).append("\n");
        if (flushPerRecord) {
            sinkBufferedWriter.flush();
        }
    }
}
