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

package org.apache.flink.connector.pulsar.testutils.sink;

import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.connector.testframe.external.sink.DataStreamSinkExternalContext;
import org.apache.flink.connector.testframe.testsuites.SinkTestSuiteBase;
import org.apache.flink.streaming.api.CheckpointingMode;

import org.junit.jupiter.api.Disabled;

/** Pulsar sink don't expose the monitor metrics now. We have to disable this test. */
public abstract class PulsarSinkTestSuiteBase extends SinkTestSuiteBase<String> {

    @Override
    @Disabled("Enable this test after FLINK-26027 being merged.")
    public void testMetrics(
            TestEnvironment testEnv,
            DataStreamSinkExternalContext<String> externalContext,
            CheckpointingMode semantic) {}
}
