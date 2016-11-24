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

package org.apache.flink.test.util;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Helper {@link SecureTestEnvironment} to handle MiniKDC lifecycle.
 * This class can be used to start/stop MiniKDC and create secure configurations for MiniDFSCluster
 * and MiniYarn
 */

public class SecureTestEnvironment {

	protected static final Logger LOG = LoggerFactory.getLogger(SecureTestEnvironment.class);

	private static MiniKdc kdc;

	private static String testKeytab = null;

	private static String testPrincipal = null;

	private static String testZkServerPrincipal = null;

	private static String testZkClientPrincipal = null;

	private static String testKafkaServerPrincipal = null;

	private static String hadoopServicePrincipal = null;

	public static void prepare(TemporaryFolder tempFolder) {

		try {
			File baseDirForSecureRun = tempFolder.newFolder();
			LOG.info("Base Directory for Secure Environment: {}", baseDirForSecureRun);

			String hostName = "localhost";
			Properties kdcConf = MiniKdc.createConf();
			if(LOG.isDebugEnabled()) {
				kdcConf.setProperty(MiniKdc.DEBUG, "true");
			}
			kdcConf.setProperty(MiniKdc.KDC_BIND_ADDRESS, hostName);
			kdc = new MiniKdc(kdcConf, baseDirForSecureRun);
			kdc.start();
			LOG.info("Started Mini KDC");

			File keytabFile = new File(baseDirForSecureRun, "test-users.keytab");
			testKeytab = keytabFile.getAbsolutePath();
			testZkServerPrincipal = "zookeeper/127.0.0.1";
			testZkClientPrincipal = "zk-client/127.0.0.1";
			testKafkaServerPrincipal = "kafka/" + hostName;
			hadoopServicePrincipal = "hadoop/" + hostName;
			testPrincipal = "client/" + hostName;

			kdc.createPrincipal(keytabFile, testPrincipal, testZkServerPrincipal,
					hadoopServicePrincipal,
					testZkClientPrincipal,
					testKafkaServerPrincipal);

			testPrincipal = testPrincipal + "@" + kdc.getRealm();
			testZkServerPrincipal = testZkServerPrincipal + "@" + kdc.getRealm();
			testZkClientPrincipal = testZkClientPrincipal + "@" + kdc.getRealm();
			testKafkaServerPrincipal = testKafkaServerPrincipal + "@" + kdc.getRealm();
			hadoopServicePrincipal = hadoopServicePrincipal + "@" + kdc.getRealm();

			LOG.info("-------------------------------------------------------------------");
			LOG.info("Test Principal: {}", testPrincipal);
			LOG.info("Test ZK Server Principal: {}", testZkServerPrincipal);
			LOG.info("Test ZK Client Principal: {}", testZkClientPrincipal);
			LOG.info("Test Kafka Server Principal: {}", testKafkaServerPrincipal);
			LOG.info("Test Hadoop Service Principal: {}", hadoopServicePrincipal);
			LOG.info("Test Keytab: {}", testKeytab);
			LOG.info("-------------------------------------------------------------------");

			//Security Context is established to allow non hadoop applications that requires JAAS
			//based SASL/Kerberos authentication to work. However, for Hadoop specific applications
			//the context can be reinitialized with Hadoop configuration by calling
			//ctx.setHadoopConfiguration() for the UGI implementation to work properly.
			//See Yarn test case module for reference
			createJaasConfig(baseDirForSecureRun);
			Configuration flinkConfig = GlobalConfiguration.loadConfiguration();
			flinkConfig.setString(ConfigConstants.SECURITY_KEYTAB_KEY, testKeytab);
			flinkConfig.setString(ConfigConstants.SECURITY_PRINCIPAL_KEY, testPrincipal);
			flinkConfig.setBoolean(HighAvailabilityOptions.ZOOKEEPER_SASL_DISABLE, false);
			SecurityUtils.SecurityConfiguration ctx = new SecurityUtils.SecurityConfiguration(flinkConfig);
			TestingSecurityContext.install(ctx, getClientSecurityConfigurationMap());

			populateJavaPropertyVariables();

		} catch(Exception e) {
			throw new RuntimeException("Exception occured while preparing secure environment.", e);
		}

	}

	public static void cleanup() {

		LOG.info("Cleaning up Secure Environment");

		if( kdc != null) {
			kdc.stop();
			LOG.info("Stopped KDC server");
		}

		resetSystemEnvVariables();

		testKeytab = null;
		testPrincipal = null;
		testZkServerPrincipal = null;
		hadoopServicePrincipal = null;

	}

	private static void populateJavaPropertyVariables() {

		if(LOG.isDebugEnabled()) {
			System.setProperty("sun.security.krb5.debug", "true");
		}

		System.setProperty("java.security.krb5.conf", kdc.getKrb5conf().getAbsolutePath());

		System.setProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
		System.setProperty("zookeeper.kerberos.removeHostFromPrincipal", "true");
		System.setProperty("zookeeper.kerberos.removeRealmFromPrincipal", "true");
	}

	private static void resetSystemEnvVariables() {
		System.clearProperty("java.security.krb5.conf");
		System.clearProperty("sun.security.krb5.debug");

		System.clearProperty("zookeeper.authProvider.1");
		System.clearProperty("zookeeper.kerberos.removeHostFromPrincipal");
		System.clearProperty("zookeeper.kerberos.removeRealmFromPrincipal");
	}

	public static org.apache.flink.configuration.Configuration populateFlinkSecureConfigurations(
			@Nullable org.apache.flink.configuration.Configuration flinkConf) {

		org.apache.flink.configuration.Configuration conf;

		if(flinkConf== null) {
			conf = new org.apache.flink.configuration.Configuration();
		} else {
			conf = flinkConf;
		}

		conf.setString(ConfigConstants.SECURITY_KEYTAB_KEY , testKeytab);
		conf.setString(ConfigConstants.SECURITY_PRINCIPAL_KEY , testPrincipal);

		return conf;
	}

	public static Map<String, TestingSecurityContext.ClientSecurityConfiguration> getClientSecurityConfigurationMap() {

		Map<String, TestingSecurityContext.ClientSecurityConfiguration> clientSecurityConfigurationMap = new HashMap<>();

		if(testZkServerPrincipal != null ) {
			TestingSecurityContext.ClientSecurityConfiguration zkServer =
					new TestingSecurityContext.ClientSecurityConfiguration(testZkServerPrincipal, testKeytab,
							"Server", "zk-server");
			clientSecurityConfigurationMap.put("Server",zkServer);
		}

		if(testZkClientPrincipal != null ) {
			TestingSecurityContext.ClientSecurityConfiguration zkClient =
					new TestingSecurityContext.ClientSecurityConfiguration(testZkClientPrincipal, testKeytab,
							"Client", "zk-client");
			clientSecurityConfigurationMap.put("Client",zkClient);
		}

		if(testKafkaServerPrincipal != null ) {
			TestingSecurityContext.ClientSecurityConfiguration kafkaServer =
					new TestingSecurityContext.ClientSecurityConfiguration(testKafkaServerPrincipal, testKeytab,
							"KafkaServer", "kafka-server");
			clientSecurityConfigurationMap.put("KafkaServer",kafkaServer);
		}

		return clientSecurityConfigurationMap;
	}

	public static String getTestKeytab() {
		return testKeytab;
	}

	public static String getHadoopServicePrincipal() {
		return hadoopServicePrincipal;
	}

	/*
	 * Helper method to create a temporary JAAS configuration file to get around the Kafka and ZK SASL
	 * implementation lookup java.security.auth.login.config
	 */
	private static void  createJaasConfig(File baseDirForSecureRun) {

		try(FileWriter fw = new FileWriter(new File(baseDirForSecureRun, SecurityUtils.JAAS_CONF_FILENAME), true);
			BufferedWriter bw = new BufferedWriter(fw);
			PrintWriter out = new PrintWriter(bw))
		{
			out.println("sample {");
			out.println("useKeyTab=false");
			out.println("useTicketCache=true;");
			out.println("};");
		} catch (IOException e) {
			throw new RuntimeException("Exception occured while trying to create JAAS config.", e);
		}

	}
}
