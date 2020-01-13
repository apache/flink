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

/** Tests that data is not inserted ahead of time. */
public class JdbcXaSinkNoInsertionTest extends JdbcXaSinkTestBase {

    @Test
    public void testNoInsertAfterInvoke() throws Exception {
        sinkHelper.emit(TEST_DATA[0]);
        assertEquals(
                "no records should be inserted for incomplete checkpoints.",
                0,
                xaHelper.countInDb());
    }

    @Test
    public void testNoInsertAfterSnapshot() throws Exception {
        sinkHelper.emitAndSnapshot(JdbcTestFixture.CP0);
        assertEquals(
                "no records should be inserted for incomplete checkpoints.",
                0,
                xaHelper.countInDb());
    }

    @Test
    public void testNoInsertAfterSinkClose() throws Exception {
        sinkHelper.emitAndSnapshot(JdbcTestFixture.CP0);
        sinkHelper.close();
        assertEquals(0, xaHelper.countInDb());
    }

    @Test
    public void testNoInsertAfterFacadeClose() throws Exception {
        try (XaFacadeImpl xaFacade = XaFacadeImpl.fromXaDataSource(xaDataSource)) {
            sinkHelper =
                    new JdbcXaSinkTestHelper(
                            buildAndInit(0, xaFacade), new TestXaSinkStateHandler());
            sinkHelper.emitAndSnapshot(JdbcTestFixture.CP0);
        }
        assertEquals(0, xaHelper.countInDb());
    }

    @Override
    protected DbMetadata getDbMetadata() {
        return JdbcTestFixture.H2_EBOOKSHOP_DB;
    }
}
