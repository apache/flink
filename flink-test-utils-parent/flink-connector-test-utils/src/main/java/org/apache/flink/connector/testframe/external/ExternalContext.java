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

package org.apache.flink.connector.testframe.external;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.source.Source;

import java.net.URL;
import java.util.List;

/**
 * External context for interacting with external system in testing framework.
 *
 * <p>An external context is responsible for provide instances and information related to an
 * external system, such as creating instance of {@link Source} and {@link Sink}, generating test
 * data, and creating data readers or writers for validating the correctness of test data.
 */
@Experimental
public interface ExternalContext extends AutoCloseable {

    /** Get URL of connector JARs that will be attached to job graphs when submitting Flink jobs. */
    List<URL> getConnectorJarPaths();
}
