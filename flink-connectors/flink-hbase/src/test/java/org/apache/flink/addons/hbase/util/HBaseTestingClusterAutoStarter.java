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

package org.apache.flink.addons.hbase.util;

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
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * By using this class as the super class of a set of tests you will have a HBase testing
 * cluster available that is very suitable for writing tests for scanning and filtering against.
 * This is usable by any downstream application because the HBase cluster is 'injected' because
 * a dynamically generated hbase-site.xml is added to the classpath.
 * Because of this classpath manipulation it is not possible to start a second testing cluster in the same JVM.
 * So if you have this you should either put all hbase related tests in a single class or force surefire to
 * setup a new JVM for each testclass.
 * See: http://maven.apache.org/surefire/maven-surefire-plugin/examples/fork-options-and-parallel-execution.html
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

	private static boolean alreadyRegisteredTestCluster = false;

	private static Configuration conf;

	protected static void createTable(TableName tableName, byte[][] columnFamilyName, byte[][] splitKeys) {
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

	private static Configuration initialize(Configuration conf) {
		conf = HBaseConfiguration.create(conf);
		conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
		try {
			admin = TEST_UTIL.getHBaseAdmin();
		} catch (MasterNotRunningException e) {
			assertNull("Master is not running", e);
		} catch (ZooKeeperConnectionException e) {
			assertNull("Cannot connect to ZooKeeper", e);
		} catch (IOException e) {
			assertNull("IOException", e);
		}
		return conf;
	}

	@BeforeClass
	public static void setUp() throws Exception {
		LOG.info("HBase minicluster: Starting");

		TEST_UTIL.startMiniCluster(1);

		// https://issues.apache.org/jira/browse/HBASE-11711
		TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", -1);

		// Make sure the zookeeper quorum value contains the right port number (varies per run).
		TEST_UTIL.getConfiguration().set("hbase.zookeeper.quorum", "localhost:" + TEST_UTIL.getZkCluster().getClientPort());

		conf = initialize(TEST_UTIL.getConfiguration());
		LOG.info("HBase minicluster: Running");
	}

	/**
	 * Returns zookeeper quorum value contains the right port number (varies per run).
	 */
	protected static String getZookeeperQuorum() {
		return "localhost:" + TEST_UTIL.getZkCluster().getClientPort();
	}

	private static File hbaseSiteXmlDirectory;
	private static File hbaseSiteXmlFile;

	/**
	 * This dynamically generates a hbase-site.xml file that is added to the classpath.
	 * This way this HBaseMinicluster can be used by an unmodified application.
	 * The downside is that this cannot be 'unloaded' so you can have only one per JVM.
	 */
	public static void registerHBaseMiniClusterInClasspath() {
		if (alreadyRegisteredTestCluster) {
			fail("You CANNOT register a second HBase Testing cluster in the classpath of the SAME JVM");
		}
		File baseDir = new File(System.getProperty("java.io.tmpdir", "/tmp/"));
		hbaseSiteXmlDirectory = new File(baseDir, "unittest-hbase-minicluster-" + Math.abs(new Random().nextLong()) + "/");

		if (!hbaseSiteXmlDirectory.mkdirs()) {
			fail("Unable to create output directory " + hbaseSiteXmlDirectory + " for the HBase minicluster");
		}

		assertNotNull("The ZooKeeper for the HBase minicluster is missing", TEST_UTIL.getZkCluster());

		createHBaseSiteXml(hbaseSiteXmlDirectory, TEST_UTIL.getConfiguration().get("hbase.zookeeper.quorum"));
		addDirectoryToClassPath(hbaseSiteXmlDirectory);

		// Avoid starting it again.
		alreadyRegisteredTestCluster = true;
	}

	public static Configuration getConf() {
		return conf;
	}

	private static void createHBaseSiteXml(File hbaseSiteXmlDirectory, String zookeeperQuorum) {
		hbaseSiteXmlFile = new File(hbaseSiteXmlDirectory, "hbase-site.xml");
		// Create the hbase-site.xml file for this run.
		try {
			String hbaseSiteXml = "<?xml version=\"1.0\"?>\n" +
				"<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n" +
				"<configuration>\n" +
				"  <property>\n" +
				"    <name>hbase.zookeeper.quorum</name>\n" +
				"    <value>" + zookeeperQuorum + "</value>\n" +
				"  </property>\n" +
				"</configuration>";
			OutputStream fos = new FileOutputStream(hbaseSiteXmlFile);
			fos.write(hbaseSiteXml.getBytes(StandardCharsets.UTF_8));
			fos.close();
		} catch (IOException e) {
			fail("Unable to create " + hbaseSiteXmlFile);
		}
	}

	private static void addDirectoryToClassPath(File directory) {
		try {
			// Get the classloader actually used by HBaseConfiguration
			ClassLoader classLoader = HBaseConfiguration.create().getClassLoader();
			if (!(classLoader instanceof URLClassLoader)) {
				fail("We should get a URLClassLoader");
			}

			// Make the addURL method accessible
			Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
			method.setAccessible(true);

			// Add the directory where we put the hbase-site.xml to the classpath
			method.invoke(classLoader, directory.toURI().toURL());
		} catch (MalformedURLException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			fail("Unable to add " + directory + " to classpath because of this exception: " + e.getMessage());
		}
	}

	@AfterClass
	public static void tearDown() throws Exception {
		LOG.info("HBase minicluster: Shutting down");
		deleteTables();
		hbaseSiteXmlFile.delete();
		hbaseSiteXmlDirectory.delete();
		TEST_UTIL.shutdownMiniCluster();
		LOG.info("HBase minicluster: Down");
	}

}
