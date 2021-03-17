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

package org.apache.flink.connector.hbase.source.hbaseendpoint;

import org.apache.flink.connector.hbase.source.reader.HBaseSourceEvent;
import org.apache.flink.connector.hbase.testutil.HBaseTestCluster;
import org.apache.flink.connector.hbase.testutil.TestsWithTestHBaseCluster;

import org.apache.hadoop.hbase.Cell;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link HBaseEndpoint}. */
public class HBaseEndpointTest extends TestsWithTestHBaseCluster {

    @Test
    public void testSetup() throws Exception {
        new HBaseEndpoint(cluster.getConfig(), cluster.getConfigurationForTable(baseTableName))
                .startReplication(
                        Collections.singletonList(HBaseTestCluster.DEFAULT_COLUMN_FAMILY));
    }

    @Test
    public void testPutCreatesEvent() throws Exception {
        cluster.makeTable(baseTableName);
        HBaseEndpoint consumer =
                new HBaseEndpoint(
                        cluster.getConfig(), cluster.getConfigurationForTable(baseTableName));
        consumer.startReplication(
                Collections.singletonList(HBaseTestCluster.DEFAULT_COLUMN_FAMILY));
        cluster.put(baseTableName, "foobar");
        List<HBaseSourceEvent> result =
                CompletableFuture.supplyAsync(consumer::getAll).get(30, TimeUnit.SECONDS);
        assertNotNull(result.get(0));
        assertEquals(Cell.Type.Put, result.get(0).getType());
    }

    @Test
    public void testDeleteCreatesEvent() throws Exception {
        cluster.makeTable(baseTableName);
        HBaseEndpoint consumer =
                new HBaseEndpoint(
                        cluster.getConfig(), cluster.getConfigurationForTable(baseTableName));
        consumer.startReplication(
                Collections.singletonList(HBaseTestCluster.DEFAULT_COLUMN_FAMILY));

        String rowKey = cluster.put(baseTableName, "foobar");
        cluster.delete(
                baseTableName,
                rowKey,
                HBaseTestCluster.DEFAULT_COLUMN_FAMILY,
                HBaseTestCluster.DEFAULT_QUALIFIER);

        List<HBaseSourceEvent> results = new ArrayList<>();
        while (results.size() < 2) {
            results.addAll(
                    CompletableFuture.supplyAsync(consumer::getAll).get(30, TimeUnit.SECONDS));
        }

        assertNotNull(results.get(1));
        assertEquals(Cell.Type.Delete, results.get(1).getType());
    }

    @Test
    public void testTimestampsAndIndicesDefineStrictOrder() throws Exception {
        HBaseEndpoint consumer =
                new HBaseEndpoint(
                        cluster.getConfig(), cluster.getConfigurationForTable(baseTableName));
        consumer.startReplication(
                Collections.singletonList(HBaseTestCluster.DEFAULT_COLUMN_FAMILY));
        cluster.makeTable(baseTableName);

        int numPuts = 3;
        int putSize = 2 * DEFAULT_COLUMNFAMILY_COUNT;
        for (int i = 0; i < numPuts; i++) {
            cluster.put(baseTableName, 1, uniqueValues(putSize));
        }

        long lastTimeStamp = -1;
        int lastIndex = -1;

        List<HBaseSourceEvent> events = new ArrayList<>();
        while (events.size() < numPuts * putSize) {
            events.addAll(
                    CompletableFuture.supplyAsync(consumer::getAll).get(30, TimeUnit.SECONDS));
        }

        for (int i = 0; i < numPuts * putSize; i++) {
            HBaseSourceEvent nextEvent = events.get(i);
            assertTrue(
                    "Events were not collected with strictly ordered, unique timestamp x index",
                    nextEvent.isLaterThan(lastTimeStamp, lastIndex));
            lastTimeStamp = nextEvent.getTimestamp();
            lastIndex = nextEvent.getIndex();
        }
    }
}
