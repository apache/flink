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

package org.apache.flink.connector.jdbc.internal.connection;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.fakedb.FakeDBUtils;
import org.apache.flink.core.testutils.CheckedThread;

import org.junit.Test;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This test deals with sql driver class loading issues, write it alone so it won't be interfered by
 * other tests.
 */
public class SimpleJdbcConnectionProviderDriverClassConcurrentLoadingTest {
    private static boolean isClassLoaded(ClassLoader classLoader, String className)
            throws Exception {
        do {
            Method m = ClassLoader.class.getDeclaredMethod("findLoadedClass", String.class);
            m.setAccessible(true);
            Object loadedClass = m.invoke(classLoader, className);
            if (loadedClass != null) {
                return true;
            }
            classLoader = classLoader.getParent();
        } while (classLoader != null);
        return false;
    }

    @Test(timeout = 5000)
    public void testDriverClassConcurrentLoading() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();

        assertFalse(isClassLoaded(classLoader, FakeDBUtils.DRIVER1_CLASS_NAME));
        assertFalse(isClassLoaded(classLoader, FakeDBUtils.DRIVER2_CLASS_NAME));

        JdbcConnectionOptions connectionOptions1 =
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(FakeDBUtils.TEST_DB_URL)
                        .withDriverName(FakeDBUtils.DRIVER1_CLASS_NAME)
                        .build();

        JdbcConnectionOptions connectionOptions2 =
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(FakeDBUtils.TEST_DB_URL)
                        .withDriverName(FakeDBUtils.DRIVER2_CLASS_NAME)
                        .build();

        CountDownLatch startLatch = new CountDownLatch(1);

        Function<JdbcConnectionOptions, CheckedThread> connectionThreadCreator =
                options -> {
                    CheckedThread thread =
                            new CheckedThread() {
                                @Override
                                public void go() throws Exception {
                                    startLatch.await();
                                    JdbcConnectionProvider connectionProvider =
                                            new SimpleJdbcConnectionProvider(options);
                                    Connection connection =
                                            connectionProvider.getOrEstablishConnection();
                                    connection.close();
                                }
                            };
                    thread.setName("Loading " + options.getDriverName());
                    thread.setDaemon(true);
                    return thread;
                };

        CheckedThread connectionThread1 = connectionThreadCreator.apply(connectionOptions1);
        CheckedThread connectionThread2 = connectionThreadCreator.apply(connectionOptions2);

        connectionThread1.start();
        connectionThread2.start();

        Thread.sleep(2);
        startLatch.countDown();

        connectionThread1.sync();
        connectionThread2.sync();

        assertTrue(isClassLoaded(classLoader, FakeDBUtils.DRIVER1_CLASS_NAME));
        assertTrue(isClassLoaded(classLoader, FakeDBUtils.DRIVER2_CLASS_NAME));
    }
}
