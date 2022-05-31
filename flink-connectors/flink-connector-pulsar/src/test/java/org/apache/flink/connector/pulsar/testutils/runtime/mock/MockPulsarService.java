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

package org.apache.flink.connector.pulsar.testutils.runtime.mock;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.zookeeper.MockZooKeeperSession;

import java.util.function.Supplier;

/** A Mock pulsar service which would use the mocked zookeeper and bookkeeper. */
public class MockPulsarService extends PulsarService {

    private final int brokerServicePort;

    private final MockZooKeeperClientFactory zooKeeperClientFactory =
            new MockZooKeeperClientFactory();

    private final MockZooKeeperSession zooKeeperSession =
            MockZooKeeperSession.newInstance(zooKeeperClientFactory.getZooKeeper());

    private final SameThreadOrderedSafeExecutor orderedExecutor =
            new SameThreadOrderedSafeExecutor();

    public MockPulsarService(ServiceConfiguration config) {
        super(config);
        this.brokerServicePort =
                config.getBrokerServicePort().orElseThrow(IllegalArgumentException::new);
    }

    public ZooKeeperClientFactory getZooKeeperClientFactory() {
        return zooKeeperClientFactory;
    }

    public BookKeeperClientFactory newBookKeeperClientFactory() {
        return new MockBookKeeperClientFactory();
    }

    public MetadataStoreExtended createLocalMetadataStore() {
        return new ZKMetadataStore(zooKeeperSession);
    }

    public MetadataStoreExtended createConfigurationMetadataStore() {
        return new ZKMetadataStore(zooKeeperSession);
    }

    public Supplier<NamespaceService> getNamespaceServiceProvider() {
        return () -> new NamespaceService(this);
    }

    @Override
    public OrderedExecutor getOrderedExecutor() {
        return orderedExecutor;
    }

    @Override
    public BrokerInterceptor getBrokerInterceptor() {
        return new BlankBrokerInterceptor();
    }

    public int getBrokerServicePort() {
        return brokerServicePort;
    }
}
