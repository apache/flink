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

package org.apache.flink.tests.util.pulsar.common;

import org.apache.flink.connector.testframe.environment.ClusterControllable;
import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase;
import org.apache.flink.streaming.api.CheckpointingMode;

import org.junit.jupiter.api.Disabled;

/** A source test template for testing the messages which could be consumed in an unordered way. */
public abstract class UnorderedSourceTestSuiteBase<T> extends SourceTestSuiteBase<T> {

    private static final String DISABLE_REASON =
            "UnorderedSourceTestSuiteBase don't support any test in SourceTestSuiteBase.";

    @Override
    @Disabled(DISABLE_REASON)
    public void testMultipleSplits(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<T> externalContext,
            CheckpointingMode semantic) {}

    @Override
    @Disabled(DISABLE_REASON)
    public void testSavepoint(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<T> externalContext,
            CheckpointingMode semantic) {}

    @Override
    @Disabled(DISABLE_REASON)
    public void testScaleUp(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<T> externalContext,
            CheckpointingMode semantic) {}

    @Override
    @Disabled(DISABLE_REASON)
    public void testScaleDown(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<T> externalContext,
            CheckpointingMode semantic) {}

    @Override
    @Disabled(DISABLE_REASON)
    public void testSourceMetrics(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<T> externalContext,
            CheckpointingMode semantic) {}

    @Override
    @Disabled(DISABLE_REASON)
    public void testIdleReader(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<T> externalContext,
            CheckpointingMode semantic) {}

    @Override
    @Disabled(DISABLE_REASON)
    public void testTaskManagerFailure(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<T> externalContext,
            ClusterControllable controller,
            CheckpointingMode semantic) {}
}
