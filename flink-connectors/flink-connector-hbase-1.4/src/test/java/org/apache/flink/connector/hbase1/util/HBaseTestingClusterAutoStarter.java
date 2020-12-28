/*
 * Copyright The Apache Software Foundation
 *
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

package org.apache.flink.connector.hbase1.util;

import org.apache.flink.test.util.AbstractTestBase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.util.VersionUtil;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * By using this class as the super class of a set of tests you will have a HBase testing cluster
 * available that is very suitable for writing tests for scanning and filtering against. This is
 * usable by any downstream application because the HBase cluster is 'injected' because a
 * dynamically generated hbase-site.xml is added to the classpath. Because of this classpath
 * manipulation it is not possible to start a second testing cluster in the same JVM. So if you have
 * this you should either put all hbase related tests in a single class or force surefire to setup a
 * new JVM for each testclass. See:
 * http://maven.apache.org/surefire/maven-surefire-plugin/examples/fork-options-and-parallel-execution.html
 */
//
// NOTE: The code in this file is based on code from the
// Apache HBase project, licensed under the Apache License v 2.0
//
// https://github.com/apache/hbase/blob/master/hbase-server/src/test/java/org/apache/hadoop/hbase/filter/FilterTestingCluster.java
//
public abstract class HBaseTestingClusterAutoStarter extends AbstractTestBase {

    private static final Log LOG = LogFactory.getLog(HBaseTestingClusterAutoStarter.class);

    private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    private static HBaseAdmin admin = null;
    private static List<TableName> createdTables = new ArrayList<>();

    private static Configuration conf;

    protected static void createTable(
            TableName tableName, byte[][] columnFamilyName, byte[][] splitKeys) {
        LOG.info("HBase minicluster: Creating table " + tableName.getNameAsString());

        assertNotNull("HBaseAdmin is not initialized successfully.", admin);
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (byte[] fam : columnFamilyName) {
            HColumnDescriptor colDef = new HColumnDescriptor(fam);
            desc.addFamily(colDef);
        }

        try {
            admin.createTable(desc, splitKeys);
            createdTables.add(tableName);
            assertTrue("Fail to create the table", admin.tableExists(tableName));
        } catch (IOException e) {
            assertNull("Exception found while creating table", e);
        }
    }

    protected static HTable openTable(TableName tableName) throws IOException {
        HTable table = (HTable) admin.getConnection().getTable(tableName);
        assertTrue("Fail to create the table", admin.tableExists(tableName));
        return table;
    }

    private static void deleteTables() {
        if (admin != null) {
            for (TableName tableName : createdTables) {
                try {
                    if (admin.tableExists(tableName)) {
                        admin.disableTable(tableName);
                        admin.deleteTable(tableName);
                    }
                } catch (IOException e) {
                    assertNull("Exception found deleting the table", e);
                }
            }
        }
    }

    private static void initialize(Configuration c) {
        conf = HBaseConfiguration.create(c);
        // the default retry number is 35 in hbase-1.4, set 5 for test
        conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5);
        try {
            admin = TEST_UTIL.getHBaseAdmin();
        } catch (MasterNotRunningException e) {
            assertNull("Master is not running", e);
        } catch (ZooKeeperConnectionException e) {
            assertNull("Cannot connect to ZooKeeper", e);
        } catch (IOException e) {
            assertNull("IOException", e);
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        // HBase 1.4 does not work with Hadoop 3
        // because it uses Guava 12.0.1, Hadoop 3 uses Guava 27.0-jre.
        // There is no Guava version in between that works with both.
        Assume.assumeTrue(
                "This test is skipped for Hadoop versions above 3",
                VersionUtil.compareVersions(System.getProperty("hadoop.version"), "3.0.0") < 0);

        LOG.info("HBase minicluster: Starting");

        TEST_UTIL.startMiniCluster(1);

        // https://issues.apache.org/jira/browse/HBASE-11711
        TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", -1);

        // Make sure the zookeeper quorum value contains the right port number (varies per run).
        LOG.info("Hbase minicluster client port: " + TEST_UTIL.getZkCluster().getClientPort());
        TEST_UTIL
                .getConfiguration()
                .set(
                        "hbase.zookeeper.quorum",
                        "localhost:" + TEST_UTIL.getZkCluster().getClientPort());

        initialize(TEST_UTIL.getConfiguration());
        LOG.info("HBase minicluster: Running");
    }

    /** Returns zookeeper quorum value contains the right port number (varies per run). */
    protected static String getZookeeperQuorum() {
        return "localhost:" + TEST_UTIL.getZkCluster().getClientPort();
    }

    public static Configuration getConf() {
        return conf;
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (conf == null) {
            LOG.info("Skipping Hbase tear down. It was never started");
            return;
        }
        LOG.info("HBase minicluster: Shutting down");
        deleteTables();
        TEST_UTIL.shutdownMiniCluster();
        LOG.info("HBase minicluster: Down");
    }
}
