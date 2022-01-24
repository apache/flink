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

package org.apache.flink.connector.kafka.testutils;

import org.apache.flink.connector.testframe.external.ExternalContextFactory;

import java.net.URL;
import java.util.List;
import java.util.function.Supplier;

/** Factory of {@link KafkaSourceExternalContext}. */
public class KafkaSourceExternalContextFactory
        implements ExternalContextFactory<KafkaSourceExternalContext> {

    private final Supplier<String> bootstrapServerSupplier;
    private final List<URL> connectorJars;
    private final KafkaSourceExternalContext.SplitMappingMode splitMappingMode;

    public KafkaSourceExternalContextFactory(
            Supplier<String> bootstrapServerSupplier,
            List<URL> connectorJars,
            KafkaSourceExternalContext.SplitMappingMode splitMappingMode) {
        this.bootstrapServerSupplier = bootstrapServerSupplier;
        this.connectorJars = connectorJars;
        this.splitMappingMode = splitMappingMode;
    }

    @Override
    public KafkaSourceExternalContext createExternalContext(String testName) {
        return new KafkaSourceExternalContext(
                bootstrapServerSupplier.get(), splitMappingMode, connectorJars);
    }
}
