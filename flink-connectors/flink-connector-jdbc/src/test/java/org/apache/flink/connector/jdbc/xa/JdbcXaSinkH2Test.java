/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.connector.jdbc.DbMetadata;
import org.apache.flink.connector.jdbc.JdbcTestFixture;

import org.junit.Test;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.junit.Assert.assertEquals;

/**
 * {@link JdbcXaSinkFunction} tests using H2 DB. H2 uses MVCC (so we can e.g. count records while
 * transaction is not yet committed). But XA support isn't full, so for some scenarios {@link
 * org.apache.flink.connector.jdbc.xa.h2.H2XaDsWrapper wrapper} is used, and for some - Derby.
 */
public class JdbcXaSinkH2Test extends JdbcXaSinkTestBase {

    @Test
    public void testIgnoreDuplicatedNotification() throws Exception {
        sinkHelper.emitAndCheckpoint(JdbcTestFixture.CP0);
        sinkHelper.notifyCheckpointComplete(JdbcTestFixture.CP0.id);
    }

    /** RM may return {@link javax.transaction.xa.XAResource#XA_RDONLY XA_RDONLY} error. */
    @Test
    public void testEmptyCheckpoint() throws Exception {
        sinkHelper.snapshotState(0);
    }

    @Test
    public void testHappyFlow() throws Exception {
        sinkHelper.emit(TEST_DATA[0]);
        assertEquals(
                "record should not be inserted before the checkpoint started.",
                0,
                xaHelper.countInDb());

        sinkHelper.snapshotState(Long.MAX_VALUE);
        assertEquals(
                "record should not be inserted before the checkpoint completed.",
                0,
                xaHelper.countInDb());

        sinkHelper.notifyCheckpointComplete(Long.MAX_VALUE);
        assertEquals(
                "record should be inserted after the checkpoint completed.",
                1,
                xaHelper.countInDb());
    }

    @Test
    public void testTwoCheckpointsWithoutData() throws Exception {
        JdbcXaSinkTestHelper sinkHelper = this.sinkHelper;
        sinkHelper.snapshotState(1);
        sinkHelper.snapshotState(2);
        assertEquals(0, xaHelper.countInDb());
    }

    @Override
    protected DbMetadata getDbMetadata() {
        return JdbcTestFixture.H2_EBOOKSHOP_DB;
    }
}
