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

package org.apache.flink.streaming.api.functions.sink.v2;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.SupportsConcurrentExecutionAttempts;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;

/**
 * A special sink that ignores all elements.
 *
 * @param <IN> The type of elements received by the sink.
 */
@PublicEvolving
public class DiscardingSink<IN> implements Sink<IN>, SupportsConcurrentExecutionAttempts {
    private static final long serialVersionUID = 1L;

    @Override
    public SinkWriter<IN> createWriter(InitContext context) throws IOException {
        return new DiscardingElementWriter();
    }

    private class DiscardingElementWriter implements SinkWriter<IN> {

        @Override
        public void write(IN element, Context context) throws IOException, InterruptedException {
            // discard it.
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            // this writer has no pending data.
        }

        @Override
        public void close() throws Exception {
            // do nothing.
        }
    }
}
