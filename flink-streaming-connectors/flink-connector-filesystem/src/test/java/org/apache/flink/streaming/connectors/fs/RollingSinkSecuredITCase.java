/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.security.SecurityContext;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.SecureTestEnvironment;
import org.apache.flink.test.util.TestingSecurityContext;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.util.NetUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY;

/**
 * Tests for running {@link RollingSinkSecuredITCase} which is an extension of {@link RollingSink} in secure environment
 */

//The test is disabled since MiniDFS secure run requires lower order ports to be used.
//We can enable the test when the fix is available (HDFS-9213)
@Ignore
public class RollingSinkSecuredITCase extends RollingSinkITCase {

	protected static final Logger LOG = LoggerFactory.getLogger(RollingSinkSecuredITCase.class);

	/*
	 * override super class static methods to avoid creating MiniDFS and MiniFlink with wrong configurations
	 * and out-of-order sequence for secure cluster
	 */
	@BeforeClass
	public static void setup() throws Exception {}

	@AfterClass
	public static void teardown() throws Exception {}

	@BeforeClass
	public static void createHDFS() throws IOException {}

	@AfterClass
	public static void destroyHDFS() {}

	@BeforeClass
	public static void startSecureCluster() throws Exception {

		LOG.info("starting secure cluster environment for testing");

		dataDir = tempFolder.newFolder();

		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dataDir.getAbsolutePath());

		SecureTestEnvironment.prepare(tempFolder);

		populateSecureConfigurations();

		Configuration flinkConfig = new Configuration();
		flinkConfig.setString(ConfigConstants.SECURITY_KEYTAB_KEY,
				SecureTestEnvironment.getTestKeytab());
		flinkConfig.setString(ConfigConstants.SECURITY_PRINCIPAL_KEY,
				SecureTestEnvironment.getHadoopServicePrincipal());

		SecurityContext.SecurityConfiguration ctx = new SecurityContext.SecurityConfiguration();
		ctx.setFlinkConfiguration(flinkConfig);
		ctx.setHadoopConfiguration(conf);
		try {
			TestingSecurityContext.install(ctx, SecureTestEnvironment.getClientSecurityConfigurationMap());
		} catch (Exception e) {
			throw new RuntimeException("Exception occurred while setting up secure test context. Reason: {}", e);
		}

		File hdfsSiteXML = new File(dataDir.getAbsolutePath() + "/hdfs-site.xml");

		FileWriter writer = new FileWriter(hdfsSiteXML);
		conf.writeXml(writer);
		writer.flush();
		writer.close();

		Map<String, String> map = new HashMap<String, String>(System.getenv());
		map.put("HADOOP_CONF_DIR", hdfsSiteXML.getParentFile().getAbsolutePath());
		TestBaseUtils.setEnv(map);


		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		builder.checkDataNodeAddrConfig(true);
		builder.checkDataNodeHostConfig(true);
		hdfsCluster = builder.build();

		dfs = hdfsCluster.getFileSystem();

		hdfsURI = "hdfs://"
				+ NetUtils.hostAndPortToUrlString(hdfsCluster.getURI().getHost(), hdfsCluster.getNameNodePort())
				+ "/";

		startSecureFlinkClusterWithRecoveryModeEnabled();
	}

	@AfterClass
	public static void teardownSecureCluster() throws Exception {
		LOG.info("tearing down secure cluster environment");

		TestStreamEnvironment.unsetAsContext();
		stopCluster(cluster, TestBaseUtils.DEFAULT_TIMEOUT);

		hdfsCluster.shutdown();

		SecureTestEnvironment.cleanup();
	}

	private static void populateSecureConfigurations() {

		String dataTransferProtection = "authentication";

		SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
		conf.set(DFS_NAMENODE_USER_NAME_KEY, SecureTestEnvironment.getHadoopServicePrincipal());
		conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, SecureTestEnvironment.getTestKeytab());
		conf.set(DFS_DATANODE_USER_NAME_KEY, SecureTestEnvironment.getHadoopServicePrincipal());
		conf.set(DFS_DATANODE_KEYTAB_FILE_KEY, SecureTestEnvironment.getTestKeytab());
		conf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, SecureTestEnvironment.getHadoopServicePrincipal());

		conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);

		conf.set("dfs.data.transfer.protection", dataTransferProtection);

		conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTP_ONLY.name());

		conf.set(DFS_ENCRYPT_DATA_TRANSFER_KEY, "false");

		conf.setInt("dfs.datanode.socket.write.timeout", 0);

		/*
		 * We ae setting the port number to privileged port - see HDFS-9213
		 * This requires the user to have root privilege to bind to the port
		 * Use below command (ubuntu) to set privilege to java process for the
		 * bind() to work if the java process is not running as root.
		 * setcap 'cap_net_bind_service=+ep' /path/to/java
		 */
		conf.set(DFS_DATANODE_ADDRESS_KEY, "localhost:1002");
		conf.set(DFS_DATANODE_HOST_NAME_KEY, "localhost");
		conf.set(DFS_DATANODE_HTTP_ADDRESS_KEY, "localhost:1003");
	}

	private static void startSecureFlinkClusterWithRecoveryModeEnabled() {
		try {
			LOG.info("Starting Flink and ZK in secure mode");

			dfs.mkdirs(new Path("/flink/checkpoints"));
			dfs.mkdirs(new Path("/flink/recovery"));

			org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();

			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, DEFAULT_PARALLELISM);
			config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, false);
			config.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, 3);
			config.setString(ConfigConstants.RECOVERY_MODE, "zookeeper");
			config.setString(ConfigConstants.STATE_BACKEND, "filesystem");
			config.setString(ConfigConstants.ZOOKEEPER_CHECKPOINTS_PATH, hdfsURI + "/flink/checkpoints");
			config.setString(ConfigConstants.ZOOKEEPER_RECOVERY_PATH, hdfsURI + "/flink/recovery");
			config.setString("state.backend.fs.checkpointdir", hdfsURI + "/flink/checkpoints");

			SecureTestEnvironment.populateFlinkSecureConfigurations(config);

			cluster = TestBaseUtils.startCluster(config, false);
			TestStreamEnvironment.setAsContext(cluster, DEFAULT_PARALLELISM);

		} catch (Exception e) {
			LOG.error("Exception occured while creating MiniFlink cluster. Reason: {}", e);
			throw new RuntimeException(e);
		}
	}

	/* For secure cluster testing, it is enough to run only one test and override below test methods
	 * to keep the overall build time minimal
	 */
	@Override
	public void testNonRollingSequenceFileWithoutCompressionWriter() throws Exception {}

	@Override
	public void testNonRollingSequenceFileWithCompressionWriter() throws Exception {}

	@Override
	public void testNonRollingAvroKeyValueWithoutCompressionWriter() throws Exception {}

	@Override
	public void testNonRollingAvroKeyValueWithCompressionWriter() throws Exception {}

	@Override
	public void testDateTimeRollingStringWriter() throws Exception {}

}