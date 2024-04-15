/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.flink.shaded.guava31.com.google.common.io.Closer;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;

/**
 * Tests for {@link ZooKeeperUtils#createTreeCache(CuratorFramework, String,
 * org.apache.flink.util.function.RunnableWithException)}.
 */
class ZooKeeperUtilsTreeCacheTest {

    private static final String PARENT_PATH = "/foo";
    private static final String CHILD_PATH = PARENT_PATH + "/bar";

    private Closer closer;
    private CuratorFramework client;
    private CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper;

    private final AtomicReference<CompletableFuture<Void>> callbackFutureReference =
            new AtomicReference<>();

    @BeforeEach
    void setUp() throws Exception {
        closer = Closer.create();
        final TestingServer testingServer =
                closer.register(ZooKeeperTestUtils.createAndStartZookeeperTestingServer());

        Configuration configuration = new Configuration();
        configuration.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, testingServer.getConnectString());

        curatorFrameworkWrapper =
                closer.register(
                        ZooKeeperUtils.startCuratorFramework(
                                configuration, NoOpFatalErrorHandler.INSTANCE));

        client = curatorFrameworkWrapper.asCuratorFramework();

        final TreeCache cache =
                closer.register(
                        ZooKeeperUtils.createTreeCache(
                                client,
                                CHILD_PATH,
                                () -> callbackFutureReference.get().complete(null)));
        cache.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        closer.close();
        callbackFutureReference.set(null);
    }

    @Test
    void testCallbackCalledOnNodeCreation() throws Exception {
        client.create().forPath(PARENT_PATH);
        callbackFutureReference.set(new CompletableFuture<>());
        client.create().forPath(CHILD_PATH);
        callbackFutureReference.get().get();
    }

    @Test
    void testCallbackCalledOnNodeModification() throws Exception {
        testCallbackCalledOnNodeCreation();

        callbackFutureReference.set(new CompletableFuture<>());
        client.setData().forPath(CHILD_PATH, new byte[1]);
        callbackFutureReference.get().get();
    }

    @Test
    void testCallbackCalledOnNodeDeletion() throws Exception {
        testCallbackCalledOnNodeCreation();

        callbackFutureReference.set(new CompletableFuture<>());
        client.delete().forPath(CHILD_PATH);
        callbackFutureReference.get().get();
    }

    @Test
    void testCallbackNotCalledOnCreationOfParents() throws Exception {
        callbackFutureReference.set(new CompletableFuture<>());
        client.create().forPath(PARENT_PATH);
        assertThatFuture(callbackFutureReference.get())
                .willNotCompleteWithin(Duration.ofMillis(20));
    }

    @Test
    void testCallbackNotCalledOnCreationOfChildren() throws Exception {
        testCallbackCalledOnNodeCreation();

        callbackFutureReference.set(new CompletableFuture<>());
        client.create().forPath(CHILD_PATH + "/baz");
        assertThatFuture(callbackFutureReference.get())
                .willNotCompleteWithin(Duration.ofMillis(20));
    }

    @Test
    void testCallbackNotCalledOnCreationOfSimilarPaths() throws Exception {
        callbackFutureReference.set(new CompletableFuture<>());
        client.create()
                .creatingParentContainersIfNeeded()
                .forPath(CHILD_PATH.substring(0, CHILD_PATH.length() - 1));
        assertThatFuture(callbackFutureReference.get())
                .willNotCompleteWithin(Duration.ofMillis(20));
    }

    @Test
    void testCallbackNotCalledOnConnectionOrInitializationEvents() throws Exception {
        final TreeCacheListener treeCacheListener =
                ZooKeeperUtils.createTreeCacheListener(
                        () -> {
                            throw new AssertionError("Should not be called.");
                        });

        treeCacheListener.childEvent(
                client, new TreeCacheEvent(TreeCacheEvent.Type.INITIALIZED, null));
        treeCacheListener.childEvent(
                client, new TreeCacheEvent(TreeCacheEvent.Type.CONNECTION_RECONNECTED, null));
        treeCacheListener.childEvent(
                client, new TreeCacheEvent(TreeCacheEvent.Type.CONNECTION_LOST, null));
        treeCacheListener.childEvent(
                client, new TreeCacheEvent(TreeCacheEvent.Type.CONNECTION_SUSPENDED, null));
    }
}
