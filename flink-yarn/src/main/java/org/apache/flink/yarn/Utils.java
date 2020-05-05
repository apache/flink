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

package org.apache.flink.yarn;

import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.yarn.YarnConfigKeys.ENV_FLINK_CLASSPATH;
import static org.apache.flink.yarn.YarnConfigKeys.LOCAL_RESOURCE_DESCRIPTOR_SEPARATOR;

/**
 * Utility class that provides helper methods to work with Apache Hadoop YARN.
 */
public final class Utils {

	private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

	/** KRB5 file name populated in YARN container for secure IT run. */
	public static final String KRB5_FILE_NAME = "krb5.conf";

	/** Yarn site xml file name populated in YARN container for secure IT run. */
	public static final String YARN_SITE_FILE_NAME = "yarn-site.xml";

	public static void setupYarnClassPath(Configuration conf, Map<String, String> appMasterEnv) {
		addToEnvironment(
			appMasterEnv,
			Environment.CLASSPATH.name(),
			appMasterEnv.get(ENV_FLINK_CLASSPATH));
		String[] applicationClassPathEntries = conf.getStrings(
			YarnConfiguration.YARN_APPLICATION_CLASSPATH,
			YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);
		for (String c : applicationClassPathEntries) {
			addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim());
		}
	}

	/**
	 * Deletes the YARN application files, e.g., Flink binaries, libraries, etc., from the remote
	 * filesystem.
	 *
	 * @param env The environment variables.
	 */
	public static void deleteApplicationFiles(final Map<String, String> env) {
		final String applicationFilesDir = env.get(YarnConfigKeys.FLINK_YARN_FILES);
		if (!StringUtils.isNullOrWhitespaceOnly(applicationFilesDir)) {
			final org.apache.flink.core.fs.Path path = new org.apache.flink.core.fs.Path(applicationFilesDir);
			try {
				final org.apache.flink.core.fs.FileSystem fileSystem = path.getFileSystem();
				if (!fileSystem.delete(path, true)) {
					LOG.error("Deleting yarn application files under {} was unsuccessful.", applicationFilesDir);
				}
			} catch (final IOException e) {
				LOG.error("Could not properly delete yarn application files directory {}.", applicationFilesDir, e);
			}
		} else {
			LOG.debug("No yarn application files directory set. Therefore, cannot clean up the data.");
		}
	}

	/**
	 * Creates a YARN resource for the remote object at the given location.
	 *
	 * @param remoteRsrcPath	remote location of the resource
	 * @param resourceSize		size of the resource
	 * @param resourceModificationTime last modification time of the resource
	 *
	 * @return YARN resource
	 */
	static LocalResource registerLocalResource(
			Path remoteRsrcPath,
			long resourceSize,
			long resourceModificationTime,
			LocalResourceVisibility resourceVisibility) {
		LocalResource localResource = Records.newRecord(LocalResource.class);
		localResource.setResource(ConverterUtils.getYarnUrlFromURI(remoteRsrcPath.toUri()));
		localResource.setSize(resourceSize);
		localResource.setTimestamp(resourceModificationTime);
		localResource.setType(LocalResourceType.FILE);
		localResource.setVisibility(resourceVisibility);
		return localResource;
	}

	/**
	 * Creates a YARN resource for the remote object at the given location.
	 * @param fs remote filesystem
	 * @param remoteRsrcPath resource path to be registered
	 * @return YARN resource
	 */
	private static LocalResource registerLocalResource(FileSystem fs, Path remoteRsrcPath) throws IOException {
		FileStatus jarStat = fs.getFileStatus(remoteRsrcPath);
		return registerLocalResource(
			remoteRsrcPath,
			jarStat.getLen(),
			jarStat.getModificationTime(),
			LocalResourceVisibility.APPLICATION);
	}

	public static void setTokensFor(ContainerLaunchContext amContainer, List<Path> paths, Configuration conf) throws IOException {
		Credentials credentials = new Credentials();
		// for HDFS
		TokenCache.obtainTokensForNamenodes(credentials, paths.toArray(new Path[0]), conf);
		// for HBase
		obtainTokenForHBase(credentials, conf);
		// for user
		UserGroupInformation currUsr = UserGroupInformation.getCurrentUser();

		Collection<Token<? extends TokenIdentifier>> usrTok = currUsr.getTokens();
		for (Token<? extends TokenIdentifier> token : usrTok) {
			final Text id = new Text(token.getIdentifier());
			LOG.info("Adding user token " + id + " with " + token);
			credentials.addToken(id, token);
		}
		try (DataOutputBuffer dob = new DataOutputBuffer()) {
			credentials.writeTokenStorageToStream(dob);

			if (LOG.isDebugEnabled()) {
				LOG.debug("Wrote tokens. Credentials buffer length: " + dob.getLength());
			}

			ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
			amContainer.setTokens(securityTokens);
		}
	}

	/**
	 * Obtain Kerberos security token for HBase.
	 */
	private static void obtainTokenForHBase(Credentials credentials, Configuration conf) throws IOException {
		if (UserGroupInformation.isSecurityEnabled()) {
			LOG.info("Attempting to obtain Kerberos security token for HBase");
			try {
				// ----
				// Intended call: HBaseConfiguration.addHbaseResources(conf);
				Class
						.forName("org.apache.hadoop.hbase.HBaseConfiguration")
						.getMethod("addHbaseResources", Configuration.class)
						.invoke(null, conf);
				// ----

				LOG.info("HBase security setting: {}", conf.get("hbase.security.authentication"));

				if (!"kerberos".equals(conf.get("hbase.security.authentication"))) {
					LOG.info("HBase has not been configured to use Kerberos.");
					return;
				}

				LOG.info("Obtaining Kerberos security token for HBase");
				// ----
				// Intended call: Token<AuthenticationTokenIdentifier> token = TokenUtil.obtainToken(conf);
				Token<?> token = (Token<?>) Class
						.forName("org.apache.hadoop.hbase.security.token.TokenUtil")
						.getMethod("obtainToken", Configuration.class)
						.invoke(null, conf);
				// ----

				if (token == null) {
					LOG.error("No Kerberos security token for HBase available");
					return;
				}

				credentials.addToken(token.getService(), token);
				LOG.info("Added HBase Kerberos security token to credentials.");
			} catch (ClassNotFoundException
					| NoSuchMethodException
					| IllegalAccessException
					| InvocationTargetException e) {
				LOG.info("HBase is not available (not packaged with this application): {} : \"{}\".",
						e.getClass().getSimpleName(), e.getMessage());
			}
		}
	}

	/**
	 * Copied method from org.apache.hadoop.yarn.util.Apps.
	 * It was broken by YARN-1824 (2.4.0) and fixed for 2.4.1
	 * by https://issues.apache.org/jira/browse/YARN-1931
	 */
	public static void addToEnvironment(Map<String, String> environment,
			String variable, String value) {
		String val = environment.get(variable);
		if (val == null) {
			val = value;
		} else {
			val = val + File.pathSeparator + value;
		}
		environment.put(StringInterner.weakIntern(variable),
				StringInterner.weakIntern(val));
	}

	/**
	 * Resolve keytab path either as absolute path or relative to working directory.
	 *
	 * @param workingDir current working directory
	 * @param keytabPath configured keytab path.
	 * @return resolved keytab path, or null if not found.
	 */
	public static String resolveKeytabPath(String workingDir, String keytabPath) {
		String keytab = null;
		if (keytabPath != null) {
			File f;
			f = new File(keytabPath);
			if (f.exists()) {
				keytab = f.getAbsolutePath();
				LOG.info("Resolved keytab path: {}", keytab);
			} else {
				// try using relative paths, this is the case when the keytab was shipped
				// as a local resource
				f = new File(workingDir, keytabPath);
				if (f.exists()) {
					keytab = f.getAbsolutePath();
					LOG.info("Resolved keytab path: {}", keytab);
				} else {
					LOG.warn("Could not resolve keytab path with: {}", keytabPath);
					keytab = null;
				}
			}
		}
		return keytab;
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private Utils() {
		throw new RuntimeException();
	}

	/**
	 * Creates the launch context, which describes how to bring up a TaskExecutor / TaskManager process in
	 * an allocated YARN container.
	 *
	 * <p>This code is extremely YARN specific and registers all the resources that the TaskExecutor
	 * needs (such as JAR file, config file, ...) and all environment variables in a YARN
	 * container launch context. The launch context then ensures that those resources will be
	 * copied into the containers transient working directory.
	 *
	 * @param flinkConfig
	 *		 The Flink configuration object.
	 * @param yarnConfig
	 *		 The YARN configuration object.
	 * @param env
	 *		 The environment variables.
	 * @param tmParams
	 *		 The TaskExecutor container memory parameters.
	 * @param taskManagerDynamicProperties
	 *		 The dynamic configurations to be updated for the TaskExecutors based on client uploaded Flink config.
	 * @param workingDirectory
	 *		 The current application master container's working directory.
	 * @param taskManagerMainClass
	 *		 The class with the main method.
	 * @param log
	 *		 The logger.
	 *
	 * @return The launch context for the TaskManager processes.
	 *
	 * @throws Exception Thrown if the launch context could not be created, for example if
	 *				   the resources could not be copied.
	 */
	static ContainerLaunchContext createTaskExecutorContext(
		org.apache.flink.configuration.Configuration flinkConfig,
		YarnConfiguration yarnConfig,
		Map<String, String> env,
		ContaineredTaskManagerParameters tmParams,
		String taskManagerDynamicProperties,
		String workingDirectory,
		Class<?> taskManagerMainClass,
		Logger log) throws Exception {

		// get and validate all relevant variables

		String remoteFlinkJarPath = env.get(YarnConfigKeys.FLINK_DIST_JAR);
		require(remoteFlinkJarPath != null, "Environment variable %s not set", YarnConfigKeys.FLINK_DIST_JAR);

		String appId = env.get(YarnConfigKeys.ENV_APP_ID);
		require(appId != null, "Environment variable %s not set", YarnConfigKeys.ENV_APP_ID);

		String clientHomeDir = env.get(YarnConfigKeys.ENV_CLIENT_HOME_DIR);
		require(clientHomeDir != null, "Environment variable %s not set", YarnConfigKeys.ENV_CLIENT_HOME_DIR);

		String shipListString = env.get(YarnConfigKeys.ENV_CLIENT_SHIP_FILES);
		require(shipListString != null, "Environment variable %s not set", YarnConfigKeys.ENV_CLIENT_SHIP_FILES);

		String yarnClientUsername = env.get(YarnConfigKeys.ENV_HADOOP_USER_NAME);
		require(yarnClientUsername != null, "Environment variable %s not set", YarnConfigKeys.ENV_HADOOP_USER_NAME);

		final String remoteKeytabPath = env.get(YarnConfigKeys.REMOTE_KEYTAB_PATH);
		final String localKeytabPath = env.get(YarnConfigKeys.LOCAL_KEYTAB_PATH);
		final String keytabPrincipal = env.get(YarnConfigKeys.KEYTAB_PRINCIPAL);
		final String remoteYarnConfPath = env.get(YarnConfigKeys.ENV_YARN_SITE_XML_PATH);
		final String remoteKrb5Path = env.get(YarnConfigKeys.ENV_KRB5_PATH);

		if (log.isDebugEnabled()) {
			log.debug("TM:remote keytab path obtained {}", remoteKeytabPath);
			log.debug("TM:local keytab path obtained {}", localKeytabPath);
			log.debug("TM:keytab principal obtained {}", keytabPrincipal);
			log.debug("TM:remote yarn conf path obtained {}", remoteYarnConfPath);
			log.debug("TM:remote krb5 path obtained {}", remoteKrb5Path);
		}

		String classPathString = env.get(ENV_FLINK_CLASSPATH);
		require(classPathString != null, "Environment variable %s not set", YarnConfigKeys.ENV_FLINK_CLASSPATH);

		//register keytab
		LocalResource keytabResource = null;
		if (remoteKeytabPath != null) {
			log.info("Adding keytab {} to the AM container local resource bucket", remoteKeytabPath);
			Path keytabPath = new Path(remoteKeytabPath);
			FileSystem fs = keytabPath.getFileSystem(yarnConfig);
			keytabResource = registerLocalResource(fs, keytabPath);
		}

		//To support Yarn Secure Integration Test Scenario
		LocalResource yarnConfResource = null;
		LocalResource krb5ConfResource = null;
		boolean hasKrb5 = false;
		if (remoteYarnConfPath != null) {
			log.info("TM:Adding remoteYarnConfPath {} to the container local resource bucket", remoteYarnConfPath);
			Path yarnConfPath = new Path(remoteYarnConfPath);
			FileSystem fs = yarnConfPath.getFileSystem(yarnConfig);
			yarnConfResource = registerLocalResource(fs, yarnConfPath);
		}

		if (remoteKrb5Path != null) {
			log.info("TM:Adding remoteKrb5Path {} to the container local resource bucket", remoteKrb5Path);
			Path krb5ConfPath = new Path(remoteKrb5Path);
			FileSystem fs = krb5ConfPath.getFileSystem(yarnConfig);
			krb5ConfResource = registerLocalResource(fs, krb5ConfPath);
			hasKrb5 = true;
		}

		Map<String, LocalResource> taskManagerLocalResources = new HashMap<>();

		// register Flink Jar with remote HDFS
		final YarnLocalResourceDescriptor flinkDistLocalResourceDesc =
				YarnLocalResourceDescriptor.fromString(remoteFlinkJarPath);
		taskManagerLocalResources.put(
				flinkDistLocalResourceDesc.getResourceKey(),
				flinkDistLocalResourceDesc.toLocalResource());

		//To support Yarn Secure Integration Test Scenario
		if (yarnConfResource != null) {
			taskManagerLocalResources.put(YARN_SITE_FILE_NAME, yarnConfResource);
		}
		if (krb5ConfResource != null) {
			taskManagerLocalResources.put(KRB5_FILE_NAME, krb5ConfResource);
		}
		if (keytabResource != null) {
			taskManagerLocalResources.put(localKeytabPath, keytabResource);
		}

		// prepare additional files to be shipped
		decodeYarnLocalResourceDescriptorListFromString(shipListString).forEach(
			resourceDesc -> taskManagerLocalResources.put(resourceDesc.getResourceKey(), resourceDesc.toLocalResource()));

		// now that all resources are prepared, we can create the launch context

		log.info("Creating container launch context for TaskManagers");

		boolean hasLogback = new File(workingDirectory, "logback.xml").exists();
		boolean hasLog4j = new File(workingDirectory, "log4j.properties").exists();

		String launchCommand = BootstrapTools.getTaskManagerShellCommand(
				flinkConfig, tmParams, ".", ApplicationConstants.LOG_DIR_EXPANSION_VAR,
				hasLogback, hasLog4j, hasKrb5, taskManagerMainClass, taskManagerDynamicProperties);

		if (log.isDebugEnabled()) {
			log.debug("Starting TaskManagers with command: " + launchCommand);
		} else {
			log.info("Starting TaskManagers");
		}

		ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
		ctx.setCommands(Collections.singletonList(launchCommand));
		ctx.setLocalResources(taskManagerLocalResources);

		Map<String, String> containerEnv = new HashMap<>();
		containerEnv.putAll(tmParams.taskManagerEnv());

		// add YARN classpath, etc to the container environment
		containerEnv.put(ENV_FLINK_CLASSPATH, classPathString);
		setupYarnClassPath(yarnConfig, containerEnv);

		containerEnv.put(YarnConfigKeys.ENV_HADOOP_USER_NAME, UserGroupInformation.getCurrentUser().getUserName());

		if (remoteKeytabPath != null && localKeytabPath != null && keytabPrincipal != null) {
			containerEnv.put(YarnConfigKeys.REMOTE_KEYTAB_PATH, remoteKeytabPath);
			containerEnv.put(YarnConfigKeys.LOCAL_KEYTAB_PATH, localKeytabPath);
			containerEnv.put(YarnConfigKeys.KEYTAB_PRINCIPAL, keytabPrincipal);
		} else if (localKeytabPath != null && keytabPrincipal != null) {
			containerEnv.put(YarnConfigKeys.LOCAL_KEYTAB_PATH, localKeytabPath);
			containerEnv.put(YarnConfigKeys.KEYTAB_PRINCIPAL, keytabPrincipal);
		}

		ctx.setEnvironment(containerEnv);

		// For TaskManager YARN container context, read the tokens from the jobmanager yarn container local file.
		// NOTE: must read the tokens from the local file, not from the UGI context, because if UGI is login
		// using Kerberos keytabs, there is no HDFS delegation token in the UGI context.
		final String fileLocation = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);

		if (fileLocation != null) {
			log.debug("Adding security tokens to TaskExecutor's container launch context.");

			try (DataOutputBuffer dob = new DataOutputBuffer()) {
				Credentials cred = Credentials.readTokenStorageFile(new File(fileLocation), HadoopUtils.getHadoopConfiguration(flinkConfig));

				// Filter out AMRMToken before setting the tokens to the TaskManager container context.
				Credentials taskManagerCred = new Credentials();
				Collection<Token<? extends TokenIdentifier>> userTokens = cred.getAllTokens();
				for (Token<? extends TokenIdentifier> token : userTokens) {
					if (!token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
						final Text id = new Text(token.getIdentifier());
						taskManagerCred.addToken(id, token);
					}
				}

				taskManagerCred.writeTokenStorageToStream(dob);
				ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
				ctx.setTokens(securityTokens);
			} catch (Throwable t) {
				log.error("Failed to add Hadoop's security tokens.", t);
			}
		} else {
			log.info("Could not set security tokens because Hadoop's token file location is unknown.");
		}

		return ctx;
	}

	static boolean isRemotePath(String path) throws IOException {
		org.apache.flink.core.fs.Path flinkPath = new org.apache.flink.core.fs.Path(path);
		return flinkPath.getFileSystem().isDistributedFS();
	}

	private static List<YarnLocalResourceDescriptor> decodeYarnLocalResourceDescriptorListFromString(String resources) throws Exception {
		final List<YarnLocalResourceDescriptor> resourceDescriptors = new ArrayList<>();
		for (String shipResourceDescStr : resources.split(LOCAL_RESOURCE_DESCRIPTOR_SEPARATOR)) {
			if (!shipResourceDescStr.isEmpty()) {
				resourceDescriptors.add(YarnLocalResourceDescriptor.fromString(shipResourceDescStr));
			}
		}
		return resourceDescriptors;
	}

	/**
	 * Validates a condition, throwing a RuntimeException if the condition is violated.
	 *
	 * @param condition The condition.
	 * @param message The message for the runtime exception, with format variables as defined by
	 *                {@link String#format(String, Object...)}.
	 * @param values The format arguments.
	 */
	static void require(boolean condition, String message, Object... values) {
		if (!condition) {
			throw new RuntimeException(String.format(message, values));
		}
	}
}
