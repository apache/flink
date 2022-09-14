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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.connector.testframe.external.ExternalContext;
import org.apache.flink.connector.testframe.external.ExternalContextFactory;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.net.URL;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The base class for Elasticsearch sink context factory base. */
public abstract class ElasticsearchSinkExternalContextFactoryBase<T extends ExternalContext>
        implements ExternalContextFactory<T> {

    /** The Elasticsearch container. */
    protected final ElasticsearchContainer elasticsearchContainer;

    /** The connector jars. */
    protected final List<URL> connectorJars;

    /**
     * Instantiates a new Elasticsearch sink context factory.
     *
     * @param elasticsearchContainer The Elasticsearch container.
     * @param connectorJars The connector jars.
     */
    ElasticsearchSinkExternalContextFactoryBase(
            ElasticsearchContainer elasticsearchContainer, List<URL> connectorJars) {
        this.elasticsearchContainer = checkNotNull(elasticsearchContainer);
        this.connectorJars = checkNotNull(connectorJars);
    }

    protected static String formatInternalAddress(
            GenericContainer<ElasticsearchContainer> container) {
        return String.format(
                "%s:%d", container.getNetworkAliases().get(0), container.getExposedPorts().get(0));
    }
}
