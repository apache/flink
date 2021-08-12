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

package org.apache.flink.connectors.test.common;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.common.junit.annotations.ExternalSystem;
import org.apache.flink.connectors.test.common.junit.annotations.TestEnv;

/**
 * Basic abstractions for all resources used in connector testing framework, including {@link
 * TestEnvironment} annotated by {@link TestEnv} and external system annotated by {@link
 * ExternalSystem}.
 *
 * <p>Lifecycle of test resources will be managed by the framework.
 */
@Experimental
public interface TestResource {

    /**
     * Start up the test resource.
     *
     * <p>The implementation of this method should be idempotent.
     *
     * @throws Exception if anything wrong when starting the resource
     */
    void startUp() throws Exception;

    /**
     * Tear down the test resource.
     *
     * <p>The test resource should be able to tear down even without a startup (could be a no-op).
     *
     * @throws Exception if anything wrong when tearing the resource down
     */
    void tearDown() throws Exception;
}
