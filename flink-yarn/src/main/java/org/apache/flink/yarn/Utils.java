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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
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
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.yarn.YarnConfigKeys.ENV_FLINK_CLASSPATH;

/**
 * Utility class that provides helper methods to work with Apache Hadoop YARN.
 */
public final class Utils {

	private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

	/** Keytab file name populated in YARN container. */
	public static final String KEYTAB_FILE_NAME = "krb5.keytab";

	/** KRB5 file name populated in YARN container for secure IT run. */
	public static final String KRB5_FILE_NAME = "krb5.conf";

	/** Yarn site xml file name populated in YARN container for secure IT run. */
	public static final String YARN_SITE_FILE_NAME = "yarn-site.xml";

	/**
	 * Define the name of flink shuffle service, it has the following usages:
	 * (1) Configure shuffle service in NodeManger in yarn-site.xml
	 * (2) Suggest the auxiliary service name of shuffle service in NodeManger
	 * (3) Yarn(Session)ResourceManager need to configure its serviceData in
	 * 		ContainerLaunchContext so that flink shuffle service will get method
	 * 		initializeApplication() being invoked. Furthermore we can add more
	 * 		information through service data if we want to add authentication
	 * 		mechanism.
	 */
	public static final String YARN_SHUFFLE_SERVICE_NAME = "yarn_shuffle_service_for_flink";

	/** Initialize this variable at the first time containFlinkShuffleService() method been called. */
	private static Boolean containFlinkShuffleService = null;

	/**
	 * See documentation.
	 */
	public static int calculateHeapSize(int memory, org.apache.flink.configuration.Configuration conf) {

		float memoryCutoffRatio = conf.getFloat(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_RATIO);
		int minCutoff = conf.getInteger(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN);

		if (memoryCutoffRatio > 1 || memoryCutoffRatio < 0) {
			throw new IllegalArgumentException("The configuration value '"
				+ ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_RATIO.key()
				+ "' must be between 0 and 1. Value given=" + memoryCutoffRatio);
		}
		if (minCutoff > memory) {
			throw new IllegalArgumentException("The configuration value '"
				+ ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN.key()
				+ "' is higher (" + minCutoff + ") than the requested amount of memory " + memory);
		}

		int heapLimit = (int) ((float) memory * memoryCutoffRatio);
		if (heapLimit < minCutoff) {
			heapLimit = minCutoff;
		}
		return memory - heapLimit;
	}

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
	 * Copy a local file to a remote file system.
	 *
	 * @param fs
	 * 		remote filesystem
	 * @param appId
	 * 		application ID
	 * @param localSrcPath
	 * 		path to the local file
	 * @param homedir
	 * 		remote home directory base (will be extended)
	 * @param relativeTargetPath
	 * 		relative target path of the file (will be prefixed be the full home directory we set up)
	 * @param preCopiedPublicPath
	 * 		the remote public path to beforehand copy
	 *
	 * @return Path to remote file (usually hdfs)
	 */
	public static Tuple2<Path, LocalResource> setupLocalResource(
		FileSystem fs,
		String appId,
		Path localSrcPath,
		Path homedir,
		String relativeTargetPath,
		@Nullable Path preCopiedPublicPath) throws IOException {

		return setupLocalResource(fs, appId, localSrcPath, homedir, relativeTargetPath, preCopiedPublicPath, null, null);
	}

	/**
	 * Copy a local file to a remote file system.
	 *
	 * @param fs
	 * 		remote filesystem
	 * @param appId
	 * 		application ID
	 * @param localSrcPath
	 * 		path to the local file
	 * @param homedir
	 * 		remote home directory base (will be extended)
	 * @param relativeTargetPath
	 * 		relative target path of the file (will be prefixed be the full home directory we set up)
	 * @param preCopiedPublicPath
	 * 		the remote public path to beforehand copy
	 * @param visibility
	 * 		local resource visibility
	 * @param resourceType
	 *    local resource type
	 *
	 * @return Path to remote file (usually hdfs)
	 */
	public static Tuple2<Path, LocalResource> setupLocalResource(
		FileSystem fs,
		String appId,
		Path localSrcPath,
		Path homedir,
		String relativeTargetPath,
		@Nullable Path preCopiedPublicPath,
		@Nullable LocalResourceVisibility visibility,
		@Nullable LocalResourceType resourceType) throws IOException {

		// default visibility
		if (visibility == null) {
			visibility = LocalResourceVisibility.APPLICATION;
		}

		if (resourceType == null) {
			resourceType = LocalResourceType.FILE;
		}

		Path dst;
		LocalResource resource;
		if (preCopiedPublicPath == null) {
			File localFile = new File(localSrcPath.toUri().getPath());
			if (localFile.isDirectory()) {
				throw new IllegalArgumentException("File to copy must not be a directory: " +
						localSrcPath);
			}
			// copy resource to HDFS
			String suffix =
				".flink/"
					+ appId
					+ (relativeTargetPath.isEmpty() ? "" : "/" + relativeTargetPath)
					+ "/" + localSrcPath.getName();

			dst = new Path(homedir, suffix);

			LOG.info("Use the local resource {}. Visibility: {}.", localSrcPath, visibility.toString());
			LOG.debug("Copying from {} to {}", localSrcPath, dst);

			fs.copyFromLocalFile(false, true, localSrcPath, dst);

			// Note: If we used registerLocalResource(FileSystem, Path) here, we would access the remote
			//       file once again which has problems with eventually consistent read-after-write file
			//       systems. Instead, we decide to preserve the modification time at the remote
			//       location because this and the size of the resource will be checked by YARN based on
			//       the values we provide to #registerLocalResource() below.
			fs.setTimes(dst, localFile.lastModified(), -1);
			// now create the resource instance
			resource = registerLocalResource(dst, localFile.length(), localFile.lastModified(), visibility, resourceType);
		} else {
			dst = preCopiedPublicPath;

			LOG.info("Use the beforehand copied resource {} (the corresponding local path: {}). Visibility: {}.", dst, localSrcPath, visibility.toString());

			resource = registerLocalResource(fs, dst, visibility, resourceType);
		}

		return Tuple2.of(dst, resource);
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

	private static LocalResource registerLocalResource(
		Path remoteRsrcPath,
		long resourceSize,
		long resourceModificationTime,
		LocalResourceVisibility visibility,
		LocalResourceType resourceType) {

		LocalResource localResource = Records.newRecord(LocalResource.class);
		localResource.setResource(ConverterUtils.getYarnUrlFromURI(remoteRsrcPath.toUri()));
		localResource.setSize(resourceSize);
		localResource.setTimestamp(resourceModificationTime);
		localResource.setType(resourceType);
		localResource.setVisibility(visibility);
		return localResource;
	}

	private static LocalResource registerLocalResource(
			Path remoteRsrcPath,
			long resourceSize,
			long resourceModificationTime,
			LocalResourceVisibility visibility) {
		return registerLocalResource(remoteRsrcPath, resourceSize, resourceModificationTime, visibility, LocalResourceType.FILE);
	}

	private static LocalResource registerLocalResource(
		FileSystem fs,
		Path remoteRsrcPath,
		LocalResourceVisibility visibility,
		LocalResourceType resourceType) throws IOException {

		FileStatus jarStat = fs.getFileStatus(remoteRsrcPath);
		return registerLocalResource(remoteRsrcPath, jarStat.getLen(), jarStat.getModificationTime(), visibility, resourceType);
	}

	private static LocalResource registerLocalResource(FileSystem fs, Path remoteRsrcPath) throws IOException {
		FileStatus jarStat = fs.getFileStatus(remoteRsrcPath);
		return registerLocalResource(remoteRsrcPath, jarStat.getLen(), jarStat.getModificationTime(), LocalResourceVisibility.APPLICATION);
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
	 * Private constructor to prevent instantiation.
	 */
	private Utils() {
		throw new RuntimeException();
	}

	/**
	 * Method to extract environment variables from the flinkConfiguration based on the given prefix String.
	 *
	 * @param envPrefix Prefix for the environment variables key
	 * @param flinkConfiguration The Flink config to get the environment variable defintion from
	 */
	public static Map<String, String> getEnvironmentVariables(String envPrefix, org.apache.flink.configuration.Configuration flinkConfiguration) {
		Map<String, String> result  = new HashMap<>();
		for (Map.Entry<String, String> entry: flinkConfiguration.toMap().entrySet()) {
			if (entry.getKey().startsWith(envPrefix) && entry.getKey().length() > envPrefix.length()) {
				// remove prefix
				String key = entry.getKey().substring(envPrefix.length());
				result.put(key, entry.getValue());
			}
		}
		return result;
	}

	public static void uploadTaskManagerConf(
		org.apache.flink.configuration.Configuration flinkConfig,
		YarnConfiguration yarnConfig,
		Map<String, String> env,
		String workingDirectory) throws IOException {

		String appId = env.get(YarnConfigKeys.ENV_APP_ID);
		require(appId != null, "Environment variable %s not set", YarnConfigKeys.ENV_APP_ID);

		String clientHomeDir = env.get(YarnConfigKeys.ENV_CLIENT_HOME_DIR);
		require(clientHomeDir != null, "Environment variable %s not set", YarnConfigKeys.ENV_CLIENT_HOME_DIR);

		org.apache.flink.configuration.Configuration taskManagerConfig = new org.apache.flink.configuration.Configuration((flinkConfig));

		// exclude tmp dir if set by env, preserve if configured by user
		if (taskManagerConfig.getString(CoreOptions.TMP_DIRS).equals(env.get(ApplicationConstants.Environment.LOCAL_DIRS.key()))) {
			taskManagerConfig.remove(CoreOptions.TMP_DIRS);
		}

		// write taskmanager configuration to file

		File taskManagerConfigFile = new File(workingDirectory, "taskmanager-conf.yaml");
		LOG.info("Writing TaskManager configuration to {}", taskManagerConfigFile.getAbsolutePath());
		BootstrapTools.writeConfiguration(taskManagerConfig, taskManagerConfigFile);

		Path homeDirPath = new Path(clientHomeDir);
		FileSystem fs = homeDirPath.getFileSystem(yarnConfig);

		// upload taskmanager config file
		Path src = new Path(taskManagerConfigFile.toURI());
		String suffix = ".flink/" + appId + "/taskmanager-conf.yaml";
		Path dst = new Path(homeDirPath, suffix);

		LOG.info("Copying from {} to {}", src, dst);
		fs.copyFromLocalFile(false, true, src, dst);
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
	 * @param taskManagerConfig
	 *		 The configuration for the TaskExecutors.
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
		org.apache.flink.configuration.Configuration taskManagerConfig,
		String workingDirectory,
		Class<?> taskManagerMainClass,
		Logger log) throws Exception {

		// get and validate all relevant variables

		String remoteFlinkJarPath = env.get(YarnConfigKeys.FLINK_JAR_PATH);
		require(remoteFlinkJarPath != null, "Environment variable %s not set", YarnConfigKeys.FLINK_JAR_PATH);

		String appId = env.get(YarnConfigKeys.ENV_APP_ID);
		require(appId != null, "Environment variable %s not set", YarnConfigKeys.ENV_APP_ID);

		String clientHomeDir = env.get(YarnConfigKeys.ENV_CLIENT_HOME_DIR);
		require(clientHomeDir != null, "Environment variable %s not set", YarnConfigKeys.ENV_CLIENT_HOME_DIR);

		String shipListString = env.get(YarnConfigKeys.ENV_CLIENT_SHIP_FILES);
		require(shipListString != null, "Environment variable %s not set", YarnConfigKeys.ENV_CLIENT_SHIP_FILES);

		String yarnClientUsername = env.get(YarnConfigKeys.ENV_HADOOP_USER_NAME);
		require(yarnClientUsername != null, "Environment variable %s not set", YarnConfigKeys.ENV_HADOOP_USER_NAME);

		final String remoteKeytabPath = env.get(YarnConfigKeys.KEYTAB_PATH);
		final String remoteKeytabPrincipal = env.get(YarnConfigKeys.KEYTAB_PRINCIPAL);
		final String remoteYarnConfPath = env.get(YarnConfigKeys.ENV_YARN_SITE_XML_PATH);
		final String remoteKrb5Path = env.get(YarnConfigKeys.ENV_KRB5_PATH);

		if (log.isDebugEnabled()) {
			log.debug("TM:remote keytab path obtained {}", remoteKeytabPath);
			log.debug("TM:remote keytab principal obtained {}", remoteKeytabPrincipal);
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
		if (remoteYarnConfPath != null && remoteKrb5Path != null) {
			log.info("TM:Adding remoteYarnConfPath {} to the container local resource bucket", remoteYarnConfPath);
			Path yarnConfPath = new Path(remoteYarnConfPath);
			FileSystem fs = yarnConfPath.getFileSystem(yarnConfig);
			yarnConfResource = registerLocalResource(fs, yarnConfPath);

			log.info("TM:Adding remoteKrb5Path {} to the container local resource bucket", remoteKrb5Path);
			Path krb5ConfPath = new Path(remoteKrb5Path);
			fs = krb5ConfPath.getFileSystem(yarnConfig);
			krb5ConfResource = registerLocalResource(fs, krb5ConfPath);

			hasKrb5 = true;
		}

		// register Flink Jar with remote HDFS
		final LocalResource flinkJar;
		{
			Path remoteJarPath = new Path(remoteFlinkJarPath);
			FileSystem fs = remoteJarPath.getFileSystem(yarnConfig);
			flinkJar = registerLocalResource(fs, remoteJarPath);
		}

		// register conf with local fs
		final LocalResource flinkConf;
		{
			Path homeDirPath = new Path(clientHomeDir);
			FileSystem fs = homeDirPath.getFileSystem(yarnConfig);

			final File taskManagerConfigFile = new File(workingDirectory, "taskmanager-conf.yaml");
			String suffix = ".flink/" + appId + "/taskmanager-conf.yaml";
			Path preCopiedPublicPath = new Path(homeDirPath, suffix);

			flinkConf = setupLocalResource(
				fs,
				appId,
				new Path(taskManagerConfigFile.toURI()),
				homeDirPath,
				"",
				preCopiedPublicPath,
				LocalResourceVisibility.APPLICATION,
				LocalResourceType.FILE).f1;

			log.debug("Prepared local resource for taskmanager config: {}", flinkConf);
		}

		Map<String, LocalResource> taskManagerLocalResources = new HashMap<>();
		taskManagerLocalResources.put("flink.jar", flinkJar);
		taskManagerLocalResources.put("flink-conf.yaml", flinkConf);

		//To support Yarn Secure Integration Test Scenario
		if (yarnConfResource != null && krb5ConfResource != null) {
			taskManagerLocalResources.put(YARN_SITE_FILE_NAME, yarnConfResource);
			taskManagerLocalResources.put(KRB5_FILE_NAME, krb5ConfResource);
		}

		if (keytabResource != null) {
			taskManagerLocalResources.put(KEYTAB_FILE_NAME, keytabResource);
		}

		// prepare additional files to be shipped
		for (String pathStr : shipListString.split(",")) {
			if (!pathStr.isEmpty()) {
				String[] keyAndPath = pathStr.split("=");
				require(keyAndPath.length == 2 || keyAndPath.length == 3, "Invalid entry in ship file list: %s", pathStr);
				Path path = new Path(keyAndPath[1]);

				LocalResource resource = null;
				if (keyAndPath.length == 3 && !keyAndPath[2].isEmpty()) {
					Map<String, String> attributeMap = new HashMap<>();
					for (String attribute : keyAndPath[2].split("\\|")) {
						String[] attKeyAndValue = attribute.split(":");
						require(attKeyAndValue.length == 2, "Invalid attribute in ship file list: %s (file: %s)", attribute, pathStr);

						attributeMap.put(attKeyAndValue[0], attKeyAndValue[1]);
					}

					LocalResourceVisibility visibility = LocalResourceVisibility.APPLICATION;
					LocalResourceType resourceType = LocalResourceType.FILE;
					final String visibilityKey = LocalResourceVisibility.class.getSimpleName();
					if (attributeMap.containsKey(visibilityKey)) {
						visibility = LocalResourceVisibility.valueOf(attributeMap.get(visibilityKey));
					}
					final String resourceTypeKey = LocalResourceType.class.getSimpleName();
					if (attributeMap.containsKey(resourceTypeKey)) {
						resourceType = LocalResourceType.valueOf(attributeMap.get(resourceTypeKey));
					}
					resource = registerLocalResource(path.getFileSystem(yarnConfig), path, visibility, resourceType);
				}
				if (resource == null){
					resource = registerLocalResource(path.getFileSystem(yarnConfig), path);
				}

				taskManagerLocalResources.put(keyAndPath[0], resource);
			}
		}

		// now that all resources are prepared, we can create the launch context

		log.info("Creating container launch context for TaskManagers");

		boolean hasLogback = new File(workingDirectory, "logback.xml").exists();
		boolean hasLog4j = new File(workingDirectory, "log4j.properties").exists();

		String launchCommand = BootstrapTools.getTaskManagerShellCommand(
				flinkConfig, tmParams, ".", ApplicationConstants.LOG_DIR_EXPANSION_VAR,
				hasLogback, hasLog4j, hasKrb5, taskManagerMainClass);

		if (log.isDebugEnabled()) {
			log.debug("Starting TaskManagers with command: " + launchCommand);
		} else {
			log.info("Starting TaskManagers");
		}

		ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
		ctx.setCommands(Collections.singletonList(launchCommand));
		ctx.setLocalResources(taskManagerLocalResources);

		if (containFlinkShuffleService()) {
			ctx.setServiceData(Collections.singletonMap(YARN_SHUFFLE_SERVICE_NAME, ByteBuffer.allocate(0)));
		}

		Map<String, String> containerEnv = new HashMap<>();
		containerEnv.putAll(tmParams.taskManagerEnv());

		// add YARN classpath, etc to the container environment
		containerEnv.put(ENV_FLINK_CLASSPATH, classPathString);
		setupYarnClassPath(yarnConfig, containerEnv);

		containerEnv.put(YarnConfigKeys.ENV_HADOOP_USER_NAME, UserGroupInformation.getCurrentUser().getUserName());

		if (remoteKeytabPath != null && remoteKeytabPrincipal != null) {
			containerEnv.put(YarnConfigKeys.KEYTAB_PATH, remoteKeytabPath);
			containerEnv.put(YarnConfigKeys.KEYTAB_PRINCIPAL, remoteKeytabPrincipal);
		}

		// overwrite taskmanager specific environment

		if (taskManagerConfig.contains(JobManagerOptions.ADDRESS)) {
			containerEnv.put(YarnConfigKeys.ENV_JM_ADDRESS, taskManagerConfig.getString(JobManagerOptions.ADDRESS));
		}
		if (taskManagerConfig.contains(JobManagerOptions.PORT)) {
			containerEnv.put(YarnConfigKeys.ENV_JM_PORT, String.valueOf(taskManagerConfig.getInteger(JobManagerOptions.PORT)));
		}
		containerEnv.put(YarnConfigKeys.ENV_TM_REGISTRATION_TIMEOUT, taskManagerConfig.getString(TaskManagerOptions.REGISTRATION_TIMEOUT));
		if (taskManagerConfig.contains(TaskManagerOptions.NUM_TASK_SLOTS)) {
			containerEnv.put(YarnConfigKeys.ENV_TM_NUM_TASK_SLOT, String.valueOf(taskManagerConfig.getInteger(TaskManagerOptions.NUM_TASK_SLOTS)));
		}
		containerEnv.put(YarnConfigKeys.ENV_TM_RESOURCE_PROFILE_KEY, taskManagerConfig.getString(TaskManagerOptions.TASK_MANAGER_RESOURCE_PROFILE_KEY));
		containerEnv.put(YarnConfigKeys.ENV_TM_MANAGED_MEMORY_SIZE, String.valueOf(taskManagerConfig.getLong(TaskManagerOptions.MANAGED_MEMORY_SIZE)));
		containerEnv.put(YarnConfigKeys.ENV_TM_NETWORK_BUFFERS_MEMORY_FRACTION, String.valueOf(taskManagerConfig.getFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION)));
		containerEnv.put(YarnConfigKeys.ENV_TM_NETWORK_BUFFERS_MEMORY_MIN, String.valueOf(taskManagerConfig.getLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN)));
		containerEnv.put(YarnConfigKeys.ENV_TM_NETWORK_BUFFERS_MEMORY_MAX, String.valueOf(taskManagerConfig.getLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX)));
		containerEnv.put(YarnConfigKeys.ENV_TM_PROCESS_NETTY_MEMORY, String.valueOf(taskManagerConfig.getInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NETTY_MEMORY)));
		containerEnv.put(YarnConfigKeys.ENV_TM_CAPACITY_CPU_CORE, String.valueOf(tmParams.taskManagerTotalCpuCore()));
		containerEnv.put(YarnConfigKeys.ENV_TM_CAPACITY_MEMORY_MB, String.valueOf(tmParams.taskManagerTotalMemoryMB()));
		ctx.setEnvironment(containerEnv);

		// For TaskManager YARN container context, read the tokens from the jobmanager yarn container local file.
		// NOTE: must read the tokens from the local file, not from the UGI context, because if UGI is login
		// using Kerberos keytabs, there is no HDFS delegation token in the UGI context.
		final String fileLocation = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);

		if (fileLocation != null) {
			log.debug("Adding security tokens to TaskExecutor's container launch context.");

			try (DataOutputBuffer dob = new DataOutputBuffer()) {
				Method readTokenStorageFileMethod = Credentials.class.getMethod(
					"readTokenStorageFile", File.class, org.apache.hadoop.conf.Configuration.class);

				Credentials cred =
					(Credentials) readTokenStorageFileMethod.invoke(
						null,
						new File(fileLocation),
						HadoopUtils.getHadoopConfiguration(flinkConfig));

				cred.writeTokenStorageToStream(dob);
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

	/**
	 * check yarn startup blink yarn shuffle service or not.
	 */
	private static Boolean containFlinkShuffleServiceInternal() {
		String services = new YarnConfiguration(new org.apache.hadoop.conf.Configuration()).get(
			"yarn.nodemanager.aux-services", "");
		Boolean ret = false;
		if (services.contains(YARN_SHUFFLE_SERVICE_NAME)) {
			ret = true;
		}
		return ret;
	}

	public static boolean containFlinkShuffleService() {
		if (containFlinkShuffleService != null) {
			return containFlinkShuffleService;
		}
		synchronized (Utils.class) {
			if (containFlinkShuffleService != null) {
				return containFlinkShuffleService;
			}
			// initialize for only once
			containFlinkShuffleService = containFlinkShuffleServiceInternal();
			return containFlinkShuffleService;
		}
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
