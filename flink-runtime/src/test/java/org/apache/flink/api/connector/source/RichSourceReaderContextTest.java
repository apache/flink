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

package org.apache.flink.api.connector.source;

import org.apache.flink.api.common.JobInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.util.UserCodeClassLoader;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link RichSourceReaderContext}. */
class RichSourceReaderContextTest {

    @Test
    void testGetJobInfoReturnsRuntimeContextJobInfo() {
        MockStreamingRuntimeContext runtimeContext = new MockStreamingRuntimeContext(1, 0);
        StubRichSourceReaderContext context = new StubRichSourceReaderContext();
        context.setRuntimeContext(runtimeContext);

        JobInfo jobInfo = context.getJobInfo();

        assertThat(jobInfo).isSameAs(runtimeContext.getJobInfo());
    }

    @Test
    void testGetJobInfoFailsWhenRuntimeContextNotSet() {
        StubRichSourceReaderContext context = new StubRichSourceReaderContext();
        assertThatThrownBy(context::getJobInfo)
                .hasMessageContaining("runtime context must not be null");
    }

    private static class StubRichSourceReaderContext extends RichSourceReaderContext {
        @Override
        public SourceReaderMetricGroup metricGroup() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Configuration getConfiguration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getLocalHostName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getIndexOfSubtask() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void sendSplitRequest() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {
            throw new UnsupportedOperationException();
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            throw new UnsupportedOperationException();
        }
    }
}
