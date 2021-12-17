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

import org.apache.derby.jdbc.EmbeddedXADataSource;
import org.junit.Test;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * {@link JdbcXaSinkFunction} tests using Derby DB. Derby supports XA but doesn't use MVCC, so we
 * can't check anything before all transactions are completed.
 */
public class JdbcXaSinkDerbyTest extends JdbcXaSinkTestBase {

    /**
     * checkpoint > capture state > emit > snapshot > close > init(captured state), open > emit >
     * checkpoint.
     */
    @Test
    public void noDuplication() throws Exception {
        sinkHelper.notifyCheckpointComplete(0);
        TestXaSinkStateHandler newState = new TestXaSinkStateHandler();
        newState.store(sinkHelper.getState().load(null));
        sinkHelper.emitAndSnapshot(JdbcTestFixture.CP0);
        sinkHelper.close(); // todo: test without close
        sinkHelper = buildSinkHelper(newState);
        sinkHelper.emitAndCheckpoint(JdbcTestFixture.CP0);
        xaHelper.assertDbContentsEquals(JdbcTestFixture.CP0);
    }

    @Test
    public void testTxEndedOnClose() throws Exception {
        sinkHelper.emit(
                TEST_DATA[0]); // don't snapshotState to prevent transaction from being prepared
        sinkHelper.close();
        xaHelper.assertPreparedTxCountEquals(0);
    }

    @Test
    public void testTxRollbackOnStartup() throws Exception {
        sinkHelper.emitAndSnapshot(JdbcTestFixture.CP0);
        xaHelper.assertPreparedTxCountEquals(1);
        sinkHelper.close();
        xaHelper.assertPreparedTxCountEquals(1);
        TestXaSinkStateHandler state =
                new TestXaSinkStateHandler(); // forget about prepared tx, so it will not commit
        // them and recover and rollback instead
        xaHelper.assertPreparedTxCountEquals(1); // tx should still be there
        buildAndInit(); // should cleanup on startup
        xaHelper.assertPreparedTxCountEquals(0);
        assertEquals(0, xaHelper.countInDb());
    }

    @Test
    public void testRestoreWithNotificationMissing() throws Exception {
        sinkHelper.emitAndSnapshot(JdbcTestFixture.CP0);
        sinkHelper.close();
        sinkHelper = buildSinkHelper(sinkHelper.getState());
        sinkHelper.emitAndCheckpoint(JdbcTestFixture.CP1);
        xaHelper.assertDbContentsEquals(JdbcTestFixture.CP0, JdbcTestFixture.CP1);
    }

    @Test
    public void testCommitUponStart() throws Exception {
        sinkHelper.emitAndSnapshot(JdbcTestFixture.CP0);
        sinkHelper.close();
        buildAndInit(0, XaFacadeImpl.fromXaDataSource(xaDataSource), sinkHelper.getState());
        xaHelper.assertDbContentsEquals(JdbcTestFixture.CP0);
    }

    /** RM may return {@link javax.transaction.xa.XAResource#XA_RDONLY XA_RDONLY} error. */
    @Test
    public void testEmptyCheckpoint() throws Exception {
        sinkHelper.snapshotState(0);
    }

    @Test
    public void testTwoCheckpointsComplete1st() throws Exception {
        sinkHelper.emitAndSnapshot(JdbcTestFixture.CP0);
        sinkHelper.emitAndSnapshot(JdbcTestFixture.CP1);
        JdbcXaSinkTestHelper sinkHelper = this.sinkHelper;
        long checkpointId = JdbcTestFixture.CP0.id;
        sinkHelper.notifyCheckpointComplete(checkpointId);
        xaHelper.cancelAllTx(); // cancel 2nd tx to prevent the following read from being blocked
        xaHelper.assertDbContentsEquals(JdbcTestFixture.CP0);
    }

    @Test
    public void testTwoCheckpointsComplete2nd() throws Exception {
        sinkHelper.emitAndSnapshot(JdbcTestFixture.CP0);
        sinkHelper.emitAndCheckpoint(JdbcTestFixture.CP1);
        xaHelper.assertDbContentsEquals(
                JdbcTestFixture.CP0,
                JdbcTestFixture.CP1); // 		"both records should be inserted after the last snapshot
        // completed."
    }

    @Test
    public void testTwoCheckpointsCompleteBoth() throws Exception {
        sinkHelper.emitAndSnapshot(JdbcTestFixture.CP0);
        sinkHelper.emitAndSnapshot(JdbcTestFixture.CP1);
        sinkHelper.notifyCheckpointComplete(JdbcTestFixture.CP0.id);
        sinkHelper.notifyCheckpointComplete(JdbcTestFixture.CP1.id);
        xaHelper.assertDbContentsEquals(
                JdbcTestFixture.CP0,
                JdbcTestFixture.CP1); // "both records should be inserted after the last snapshot
        // completed."
    }

    @Test
    public void testTwoCheckpointsCompleteBothOutOfOrder() throws Exception {
        sinkHelper.emitAndSnapshot(JdbcTestFixture.CP0);
        sinkHelper.emitAndSnapshot(JdbcTestFixture.CP1);
        sinkHelper.notifyCheckpointComplete(JdbcTestFixture.CP1.id);
        sinkHelper.notifyCheckpointComplete(JdbcTestFixture.CP0.id);
        xaHelper.assertDbContentsEquals(
                JdbcTestFixture.CP0,
                JdbcTestFixture.CP1); // "both records should be inserted after the last snapshot
        // completed."
    }

    @Test
    public void testRestore() throws Exception {
        sinkHelper.emitAndCheckpoint(JdbcTestFixture.CP0);
        sinkHelper.close();
        sinkHelper = new JdbcXaSinkTestHelper(buildAndInit(), new TestXaSinkStateHandler());
        sinkHelper.emitAndCheckpoint(JdbcTestFixture.CP1);
        xaHelper.assertDbContentsEquals(JdbcTestFixture.CP0, JdbcTestFixture.CP1);
    }

    @Test
    public void testFailurePropagation() throws Exception {
        /* big enough flush interval to cause error in snapshotState rather than in invoke*/
        sinkHelper =
                new JdbcXaSinkTestHelper(
                        buildAndInit(
                                Integer.MAX_VALUE, XaFacadeImpl.fromXaDataSource(xaDataSource)),
                        new TestXaSinkStateHandler());
        sinkHelper.emit(TEST_DATA[0]);
        sinkHelper.emit(TEST_DATA[0]); // duplicate
        try {
            sinkHelper.snapshotState(0);
        } catch (Exception e) {
            // expected: flush or commit duplicated records
            return;
        }
        fail("should propagate error from snapshotState");
    }

    static EmbeddedXADataSource derbyXaDs() {
        EmbeddedXADataSource ds = new EmbeddedXADataSource();
        ds.setDatabaseName(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getDbName());
        return ds;
    }

    @Override
    protected DbMetadata getDbMetadata() {
        return JdbcTestFixture.DERBY_EBOOKSHOP_DB;
    }
}
