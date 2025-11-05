/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.connector.sink.lib;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.operators.sink.InitContextBase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/** A {@link Sink} that reads data using an {@link OutputFormat}. */
@Internal
public class OutputFormatSink<IN> implements Sink<IN> {
    private final OutputFormat<IN> format;

    public OutputFormatSink(OutputFormat<IN> format) {
        this.format = format;
    }

    @Override
    public SinkWriter<IN> createWriter(WriterInitContext writerContext) throws IOException {
        RuntimeContext runtimeContext = null;
        if (writerContext instanceof InitContextBase) {
            runtimeContext = ((InitContextBase) writerContext).getRuntimeContext();
        }
        return new InputFormatSinkWriter<>(writerContext, format, runtimeContext);
    }

    private static class InputFormatSinkWriter<IN> implements SinkWriter<IN> {
        private static final Logger LOG = LoggerFactory.getLogger(InputFormatSinkWriter.class);

        private final OutputFormat<IN> format;
        private boolean cleanupCalled = false;

        public InputFormatSinkWriter(
                WriterInitContext writerContext,
                OutputFormat<IN> format,
                @Nullable RuntimeContext runtimeContext)
                throws IOException {
            this.format = format;

            if (format instanceof RichOutputFormat) {
                ((RichOutputFormat<?>) format).setRuntimeContext(runtimeContext);
            }
            if (runtimeContext instanceof StreamingRuntimeContext) {
                format.configure(((StreamingRuntimeContext) runtimeContext).getJobConfiguration());
            } else {
                format.configure(new Configuration());
            }

            int indexInSubtaskGroup = writerContext.getTaskInfo().getIndexOfThisSubtask();
            int currentNumberOfSubtasks = writerContext.getTaskInfo().getNumberOfParallelSubtasks();
            format.open(
                    new OutputFormat.InitializationContext() {
                        @Override
                        public int getNumTasks() {
                            return currentNumberOfSubtasks;
                        }

                        @Override
                        public int getTaskNumber() {
                            return indexInSubtaskGroup;
                        }

                        @Override
                        public int getAttemptNumber() {
                            return writerContext.getTaskInfo().getAttemptNumber();
                        }
                    });
        }

        @Override
        public void write(IN element, Context context) throws IOException {
            try {
                format.writeRecord(element);
            } catch (Exception ex) {
                cleanup();
                throw ex;
            }
        }

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public void close() throws Exception {
            try {
                format.close();
            } catch (Exception ex) {
                cleanup();
                throw ex;
            }
        }

        private void cleanup() {
            try {
                if (!cleanupCalled && format instanceof CleanupWhenUnsuccessful) {
                    cleanupCalled = true;
                    ((CleanupWhenUnsuccessful) format).tryCleanupOnError();
                }
            } catch (Throwable t) {
                LOG.error("Cleanup on error failed.", t);
            }
        }
    }
}
