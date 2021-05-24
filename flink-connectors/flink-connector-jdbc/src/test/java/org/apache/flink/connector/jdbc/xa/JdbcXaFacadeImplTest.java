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
import org.apache.flink.connector.jdbc.JdbcTestBase;
import org.apache.flink.connector.jdbc.JdbcTestFixture;

import org.junit.Test;

import javax.transaction.xa.Xid;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/** {@link XaFacadeImpl} tests. */
public class JdbcXaFacadeImplTest extends JdbcTestBase {

    private static final Xid XID =
            new Xid() {
                @Override
                public int getFormatId() {
                    return 0;
                }

                @Override
                public byte[] getGlobalTransactionId() {
                    return "01234".getBytes();
                }

                @Override
                public byte[] getBranchQualifier() {
                    return "56789".getBytes();
                }
            };

    @Test
    public void testRecover() throws Exception {
        try (XaFacade f = XaFacadeImpl.fromXaDataSource(getDbMetadata().buildXaDataSource())) {
            f.open();
            assertEquals(0, f.recover().size());
            f.start(XID);
            // insert some data to prevent database from ignoring the transaction
            try (Connection c = f.getConnection()) {
                try (Statement s = c.createStatement()) {
                    s.executeUpdate(JdbcTestFixture.getInsertQuery());
                }
            }
            f.endAndPrepare(XID);
        }
        try (XaFacade f = XaFacadeImpl.fromXaDataSource(getDbMetadata().buildXaDataSource())) {
            f.open();
            Collection<Xid> recovered = f.recover();
            recovered.forEach(f::rollback);
            assertEquals(1, recovered.size());
        }
    }

    @Override
    protected DbMetadata getDbMetadata() {
        return JdbcTestFixture.DERBY_EBOOKSHOP_DB;
    }
}
