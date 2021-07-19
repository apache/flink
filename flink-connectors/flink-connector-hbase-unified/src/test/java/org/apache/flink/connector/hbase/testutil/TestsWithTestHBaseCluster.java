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

package org.apache.flink.connector.hbase.testutil;

import org.junit.Before;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/** Abstract test class that provides that {@link HBaseTestCluster} is up and running. */
public abstract class TestsWithTestHBaseCluster {

    protected static final Logger LOG = LoggerFactory.getLogger(TestsWithTestHBaseCluster.class);
    public static final int DEFAULT_COLUMNFAMILY_COUNT = 4;

    @Rule public FileSignal testOracle = new FileSignal();
    @Rule public HBaseTestCluster cluster = new HBaseTestCluster();

    /** Shadowed from org.apache.flink.test.util.SuccessException. */
    public static class SuccessException extends RuntimeException {}

    /**
     * Unique table name provided for each test; can be used to minimize interference between tests
     * on the same cluster.
     */
    protected String baseTableName;

    @Before
    public void determineBaseTableName() {
        baseTableName =
                String.format(
                        "%s-table-%s", getClass().getSimpleName().toLowerCase(), UUID.randomUUID());
    }

    protected static boolean causedBySuccess(Exception exception) {
        for (Throwable e = exception; e != null; e = e.getCause()) {
            if (e instanceof SuccessException) {
                return true;
            }
        }
        return false;
    }

    protected static String[] uniqueValues(int count) {
        String[] values = new String[count];
        for (int i = 0; i < count; i++) {
            values[i] = UUID.randomUUID().toString();
        }
        return values;
    }

    protected static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException("Waiting was interrupted", e);
        }
    }
}
