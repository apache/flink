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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.entrypoint.parser.CommandLineOptions;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.OperatingSystem;

import org.apache.flink.shaded.guava18.com.google.common.escape.Escaper;
import org.apache.flink.shaded.guava18.com.google.common.escape.Escapers;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelException;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.BindException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import scala.Some;
import scala.Tuple2;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Tools for starting JobManager and TaskManager processes, including the
 * Actor Systems used to run the JobManager and TaskManager actors.
 */
public class BootstrapTools {
	/**
	 * Internal option which says if default value is used for {@link CoreOptions#TMP_DIRS}.
	 */
	private static final ConfigOption<Boolean> USE_LOCAL_DEFAULT_TMP_DIRS = key("internal.io.tmpdirs.use-local-default")
		.defaultValue(false);

	private static final Logger LOG = LoggerFactory.getLogger(BootstrapTools.class);

	private static final Escaper UNIX_SINGLE_QUOTE_ESCAPER = Escapers.builder()
		.addEscape('\'', "'\\''")
		.build();

	private static final Escaper WINDOWS_DOUBLE_QUOTE_ESCAPER = Escapers.builder()
		.addEscape('"', "\\\"")
		.addEscape('^', "\"^^\"")
		.build();

	/**
	 * Starts a remote ActorSystem at given address and specific port range.
	 * @param configuration The Flink configuration
	 * @param externalAddress The external address to access the ActorSystem.
	 * @param externalPortRange The choosing range of the external port to access the ActorSystem.
	 * @param logger The logger to output log information.
	 * @return The ActorSystem which has been started
	 * @throws Exception Thrown when actor system cannot be started in specified port range
	 */
	@VisibleForTesting
	public static ActorSystem startRemoteActorSystem(
		Configuration configuration,
		String externalAddress,
		String externalPortRange,
		Logger logger) throws Exception {
		return startRemoteActorSystem(
			configuration,
			AkkaUtils.getFlinkActorSystemName(),
			externalAddress,
			externalPortRange,
			NetUtils.getWildcardIPAddress(),
			Optional.empty(),
			logger,
			ForkJoinExecutorConfiguration.fromConfiguration(configuration),
			null);
	}

	/**
	 * Starts a remote ActorSystem at given address and specific port range.
	 *
	 * @param configuration The Flink configuration
	 * @param actorSystemName Name of the started {@link ActorSystem}
	 * @param externalAddress The external address to access the ActorSystem.
	 * @param externalPortRange The choosing range of the external port to access the ActorSystem.
	 * @param bindAddress The local address to bind to.
	 * @param bindPort The local port to bind to. If not present, then the external port will be used.
	 * @param logger The logger to output log information.
	 * @param actorSystemExecutorConfiguration configuration for the ActorSystem's underlying executor
	 * @param customConfig Custom Akka config to be combined with the config derived from Flink configuration.
	 * @return The ActorSystem which has been started
	 * @throws Exception Thrown when actor system cannot be started in specified port range
	 */
	public static ActorSystem startRemoteActorSystem(
			Configuration configuration,
			String actorSystemName,
			String externalAddress,
			String externalPortRange,
			String bindAddress,
			@SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<Integer> bindPort,
			Logger logger,
			ActorSystemExecutorConfiguration actorSystemExecutorConfiguration,
			Config customConfig) throws Exception {

		// parse port range definition and create port iterator
		Iterator<Integer> portsIterator;
		try {
			portsIterator = NetUtils.getPortRangeFromString(externalPortRange);
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid port range definition: " + externalPortRange);
		}

		while (portsIterator.hasNext()) {
			final int externalPort = portsIterator.next();

			try {
				return startRemoteActorSystem(
					configuration,
					actorSystemName,
					externalAddress,
					externalPort,
					bindAddress,
					bindPort.orElse(externalPort),
					logger,
					actorSystemExecutorConfiguration,
					customConfig);
			}
			catch (Exception e) {
				// we can continue to try if this contains a netty channel exception
				Throwable cause = e.getCause();
				if (!(cause instanceof org.jboss.netty.channel.ChannelException ||
						cause instanceof java.net.BindException)) {
					throw e;
				} // else fall through the loop and try the next port
			}
		}

		// if we come here, we have exhausted the port range
		throw new BindException("Could not start actor system on any port in port range "
			+ externalPortRange);
	}

	/**
	 * Starts a remote Actor System at given address and specific port.
	 * @param configuration The Flink configuration.
	 * @param actorSystemName Name of the started {@link ActorSystem}
	 * @param externalAddress The external address to access the ActorSystem.
	 * @param externalPort The external port to access the ActorSystem.
	 * @param bindAddress The local address to bind to.
	 * @param bindPort The local port to bind to.
	 * @param logger the logger to output log information.
	 * @param actorSystemExecutorConfiguration configuration for the ActorSystem's underlying executor
	 * @param customConfig Custom Akka config to be combined with the config derived from Flink configuration.
	 * @return The ActorSystem which has been started.
	 * @throws Exception
	 */
	private static ActorSystem startRemoteActorSystem(
		Configuration configuration,
		String actorSystemName,
		String externalAddress,
		int externalPort,
		String bindAddress,
		int bindPort,
		Logger logger,
		ActorSystemExecutorConfiguration actorSystemExecutorConfiguration,
		Config customConfig) throws Exception {

		String externalHostPortUrl = NetUtils.unresolvedHostAndPortToNormalizedString(externalAddress, externalPort);
		String bindHostPortUrl = NetUtils.unresolvedHostAndPortToNormalizedString(bindAddress, bindPort);
		logger.info("Trying to start actor system, external address {}, bind address {}.", externalHostPortUrl, bindHostPortUrl);

		try {
			Config akkaConfig = AkkaUtils.getAkkaConfig(
				configuration,
				new Some<>(new Tuple2<>(externalAddress, externalPort)),
				new Some<>(new Tuple2<>(bindAddress, bindPort)),
				actorSystemExecutorConfiguration.getAkkaConfig());

			if (customConfig != null) {
				akkaConfig = customConfig.withFallback(akkaConfig);
			}

			return startActorSystem(akkaConfig, actorSystemName, logger);
		}
		catch (Throwable t) {
			if (t instanceof ChannelException) {
				Throwable cause = t.getCause();
				if (cause != null && t.getCause() instanceof BindException) {
					throw new IOException("Unable to create ActorSystem at address " + bindHostPortUrl +
						" : " + cause.getMessage(), t);
				}
			}
			throw new Exception("Could not create actor system", t);
		}
	}

	/**
	 * Starts a local Actor System.
	 * @param configuration The Flink configuration.
	 * @param actorSystemName Name of the started ActorSystem.
	 * @param logger The logger to output log information.
	 * @param actorSystemExecutorConfiguration Configuration for the ActorSystem's underlying executor.
	 * @param customConfig Custom Akka config to be combined with the config derived from Flink configuration.
	 * @return The ActorSystem which has been started.
	 * @throws Exception
	 */
	public static ActorSystem startLocalActorSystem(
		Configuration configuration,
		String actorSystemName,
		Logger logger,
		ActorSystemExecutorConfiguration actorSystemExecutorConfiguration,
		Config customConfig) throws Exception {

		logger.info("Trying to start local actor system");

		try {
			Config akkaConfig = AkkaUtils.getAkkaConfig(
				configuration,
				scala.Option.empty(),
				scala.Option.empty(),
				actorSystemExecutorConfiguration.getAkkaConfig());

			if (customConfig != null) {
				akkaConfig = customConfig.withFallback(akkaConfig);
			}

			return startActorSystem(akkaConfig, actorSystemName, logger);
		}
		catch (Throwable t) {
			throw new Exception("Could not create actor system", t);
		}
	}

	/**
	 * Starts an Actor System with given Akka config.
	 * @param akkaConfig Config of the started ActorSystem.
	 * @param actorSystemName Name of the started ActorSystem.
	 * @param logger The logger to output log information.
	 * @return The ActorSystem which has been started.
	 */
	private static ActorSystem startActorSystem(Config akkaConfig, String actorSystemName, Logger logger) {
		logger.debug("Using akka configuration\n {}", akkaConfig);
		ActorSystem actorSystem = AkkaUtils.createActorSystem(actorSystemName, akkaConfig);

		logger.info("Actor system started at {}", AkkaUtils.getAddress(actorSystem));
		return actorSystem;
	}

	/**
	 * Writes a Flink YAML config file from a Flink Configuration object.
	 * @param cfg The Flink config
	 * @param file The File to write to
	 * @throws IOException
	 */
	public static void writeConfiguration(Configuration cfg, File file) throws IOException {
		try (FileWriter fwrt = new FileWriter(file);
			PrintWriter out = new PrintWriter(fwrt)) {
			for (Map.Entry<String, String> entry : cfg.toMap().entrySet()) {
				out.print(entry.getKey());
				out.print(": ");
				out.println(entry.getValue());
			}
		}
	}

	/**
	* Sets the value of a new config key to the value of a deprecated config key.
	* @param config Config to write
	* @param deprecated The old config key
	* @param designated The new config key
	*/
	public static void substituteDeprecatedConfigKey(Configuration config, String deprecated, String designated) {
		// set the designated key only if it is not set already
		if (!config.containsKey(designated)) {
			final String valueForDeprecated = config.getString(deprecated, null);
			if (valueForDeprecated != null) {
				config.setString(designated, valueForDeprecated);
			}
		}
	}

	/**
	* Sets the value of a new config key to the value of a deprecated config key. Taking into
	* account the changed prefix.
	* @param config Config to write
	* @param deprecatedPrefix Old prefix of key
	* @param designatedPrefix New prefix of key
	*/
	public static void substituteDeprecatedConfigPrefix(
			Configuration config,
			String deprecatedPrefix,
			String designatedPrefix) {

		// set the designated key only if it is not set already
		final int prefixLen = deprecatedPrefix.length();

		Configuration replacement = new Configuration();

		for (String key : config.keySet()) {
			if (key.startsWith(deprecatedPrefix)) {
				String newKey = designatedPrefix + key.substring(prefixLen);
				if (!config.containsKey(newKey)) {
					replacement.setString(newKey, config.getString(key, null));
				}
			}
		}

		config.addAll(replacement);
	}

	private static final String DYNAMIC_PROPERTIES_OPT = "D";

	/**
	 * Get an instance of the dynamic properties option.
	 *
	 * <p>Dynamic properties allow the user to specify additional configuration values with -D, such as
	 * <tt> -Dfs.overwrite-files=true  -Dtaskmanager.memory.network.min=536346624</tt>
     */
	public static Option newDynamicPropertiesOption() {
		return new Option(DYNAMIC_PROPERTIES_OPT, true, "Dynamic properties");
	}

	/**
	 * Parse the dynamic properties (passed on the command line).
	 */
	public static Configuration parseDynamicProperties(CommandLine cmd) {
		final Configuration config = new Configuration();

		String[] values = cmd.getOptionValues(DYNAMIC_PROPERTIES_OPT);
		if (values != null) {
			for (String value : values) {
				String[] pair = value.split("=", 2);
				if (pair.length == 1) {
					config.setString(pair[0], Boolean.TRUE.toString());
				}
				else if (pair.length == 2) {
					config.setString(pair[0], pair[1]);
				}
			}
		}

		return config;
	}

	/**
	 * Generates the shell command to start a task manager.
	 * @param flinkConfig The Flink configuration.
	 * @param tmParams Parameters for the task manager.
	 * @param configDirectory The configuration directory for the flink-conf.yaml
	 * @param logDirectory The log directory.
	 * @param hasLogback Uses logback?
	 * @param hasLog4j Uses log4j?
	 * @param mainClass The main class to start with.
	 * @return A String containing the task manager startup command.
	 */
	public static String getTaskManagerShellCommand(
			Configuration flinkConfig,
			ContaineredTaskManagerParameters tmParams,
			String configDirectory,
			String logDirectory,
			boolean hasLogback,
			boolean hasLog4j,
			boolean hasKrb5,
			Class<?> mainClass,
			String mainArgs) {

		final Map<String, String> startCommandValues = new HashMap<>();
		startCommandValues.put("java", "$JAVA_HOME/bin/java");

		final TaskExecutorProcessSpec taskExecutorProcessSpec = tmParams.getTaskExecutorProcessSpec();
		startCommandValues.put("jvmmem", ProcessMemoryUtils.generateJvmParametersStr(taskExecutorProcessSpec));

		String javaOpts = flinkConfig.getString(CoreOptions.FLINK_JVM_OPTIONS);
		if (flinkConfig.getString(CoreOptions.FLINK_TM_JVM_OPTIONS).length() > 0) {
			javaOpts += " " + flinkConfig.getString(CoreOptions.FLINK_TM_JVM_OPTIONS);
		}
		//applicable only for YarnMiniCluster secure test run
		//krb5.conf file will be available as local resource in JM/TM container
		if (hasKrb5) {
			javaOpts += " -Djava.security.krb5.conf=krb5.conf";
		}
		startCommandValues.put("jvmopts", javaOpts);

		String logging = "";
		if (hasLogback || hasLog4j) {
			logging = "-Dlog.file=" + logDirectory + "/taskmanager.log";
			if (hasLogback) {
				logging +=
					" -Dlogback.configurationFile=file:" + configDirectory +
						"/logback.xml";
			}
			if (hasLog4j) {
				logging += " -Dlog4j.configuration=file:" + configDirectory +
					"/log4j.properties";
				logging += " -Dlog4j.configurationFile=file:" + configDirectory +
					"/log4j.properties";
			}
		}

		startCommandValues.put("logging", logging);
		startCommandValues.put("class", mainClass.getName());
		startCommandValues.put("redirects",
			"1> " + logDirectory + "/taskmanager.out " +
			"2> " + logDirectory + "/taskmanager.err");

		String argsStr = TaskExecutorProcessUtils.generateDynamicConfigsStr(taskExecutorProcessSpec) + " --configDir " + configDirectory;
		if (!mainArgs.isEmpty()) {
			argsStr += " " + mainArgs;
		}
		startCommandValues.put("args", argsStr);

		final String commandTemplate = flinkConfig
			.getString(ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,
				ConfigConstants.DEFAULT_YARN_CONTAINER_START_COMMAND_TEMPLATE);
		String startCommand = getStartCommand(commandTemplate, startCommandValues);
		LOG.debug("TaskManager start command: " + startCommand);

		return startCommand;
	}


	// ------------------------------------------------------------------------

	/** Private constructor to prevent instantiation. */
	private BootstrapTools() {}

	/**
	 * Replaces placeholders in the template start command with values from startCommandValues.
	 *
	 * <p>If the default template {@link ConfigConstants#DEFAULT_YARN_CONTAINER_START_COMMAND_TEMPLATE}
	 * is used, the following keys must be present in the map or the resulting
	 * command will still contain placeholders:
	 * <ul>
	 * <li><tt>java</tt> = path to the Java executable</li>
	 * <li><tt>jvmmem</tt> = JVM memory limits and tweaks</li>
	 * <li><tt>jvmopts</tt> = misc options for the Java VM</li>
	 * <li><tt>logging</tt> = logging-related configuration settings</li>
	 * <li><tt>class</tt> = main class to execute</li>
	 * <li><tt>args</tt> = arguments for the main class</li>
	 * <li><tt>redirects</tt> = output redirects</li>
	 * </ul>
	 *
	 * @param template
	 * 		a template start command with placeholders
	 * @param startCommandValues
	 * 		a replacement map <tt>placeholder -&gt; value</tt>
	 *
	 * @return the start command with placeholders filled in
	 */
	public static String getStartCommand(String template,
		Map<String, String> startCommandValues) {
		for (Map.Entry<String, String> variable : startCommandValues
			.entrySet()) {
			template = template
				.replace("%" + variable.getKey() + "%", variable.getValue())
				.replace("  ", " ")
				.trim();
		}
		return template;
	}

	/**
	 * Set temporary configuration directories if necessary.
	 *
	 * @param configuration flink config to patch
	 * @param defaultDirs in case no tmp directories is set, next directories will be applied
	 */
	public static void updateTmpDirectoriesInConfiguration(
			Configuration configuration,
			@Nullable String defaultDirs) {
		if (configuration.contains(CoreOptions.TMP_DIRS)) {
			LOG.info("Overriding Fink's temporary file directories with those " +
				"specified in the Flink config: {}", configuration.getValue(CoreOptions.TMP_DIRS));
		} else if (defaultDirs != null) {
			LOG.info("Setting directories for temporary files to: {}", defaultDirs);
			configuration.setString(CoreOptions.TMP_DIRS, defaultDirs);
			configuration.setBoolean(USE_LOCAL_DEFAULT_TMP_DIRS, true);
		}
	}

	/**
	 * Clones the given configuration and resets instance specific config options.
	 *
	 * @param configuration to clone
	 * @return Cloned configuration with reset instance specific config options
	 */
	public static Configuration cloneConfiguration(Configuration configuration) {
		final Configuration clonedConfiguration = new Configuration(configuration);

		if (clonedConfiguration.getBoolean(USE_LOCAL_DEFAULT_TMP_DIRS)){
			clonedConfiguration.removeConfig(CoreOptions.TMP_DIRS);
			clonedConfiguration.removeConfig(USE_LOCAL_DEFAULT_TMP_DIRS);
		}

		return clonedConfiguration;
	}

	/**
	 * Configuration interface for {@link ActorSystem} underlying executor.
	 */
	public interface ActorSystemExecutorConfiguration {

		/**
		 * Create the executor {@link Config} for the respective executor.
		 *
		 * @return Akka config for the respective executor
		 */
		Config getAkkaConfig();
	}

	/**
	 * Configuration for a fork join executor.
	 */
	public static class ForkJoinExecutorConfiguration implements ActorSystemExecutorConfiguration {

		private final double parallelismFactor;

		private final int minParallelism;

		private final int maxParallelism;

		public ForkJoinExecutorConfiguration(double parallelismFactor, int minParallelism, int maxParallelism) {
			this.parallelismFactor = parallelismFactor;
			this.minParallelism = minParallelism;
			this.maxParallelism = maxParallelism;
		}

		public double getParallelismFactor() {
			return parallelismFactor;
		}

		public int getMinParallelism() {
			return minParallelism;
		}

		public int getMaxParallelism() {
			return maxParallelism;
		}

		@Override
		public Config getAkkaConfig() {
			return AkkaUtils.getForkJoinExecutorConfig(this);
		}

		public static ForkJoinExecutorConfiguration fromConfiguration(final Configuration configuration) {
			final double parallelismFactor = configuration.getDouble(AkkaOptions.FORK_JOIN_EXECUTOR_PARALLELISM_FACTOR);
			final int minParallelism = configuration.getInteger(AkkaOptions.FORK_JOIN_EXECUTOR_PARALLELISM_MIN);
			final int maxParallelism = configuration.getInteger(AkkaOptions.FORK_JOIN_EXECUTOR_PARALLELISM_MAX);

			return new ForkJoinExecutorConfiguration(parallelismFactor, minParallelism, maxParallelism);
		}
	}

	/**
	 * Configuration for a fixed thread pool executor.
	 */
	public static class FixedThreadPoolExecutorConfiguration implements ActorSystemExecutorConfiguration {

		private final int minNumThreads;

		private final int maxNumThreads;

		private final int threadPriority;

		public FixedThreadPoolExecutorConfiguration(int minNumThreads, int maxNumThreads, int threadPriority) {
			if (threadPriority < Thread.MIN_PRIORITY || threadPriority > Thread.MAX_PRIORITY) {
				throw new IllegalArgumentException(
					String.format(
						"The thread priority must be within (%s, %s) but it was %s.",
						Thread.MIN_PRIORITY,
						Thread.MAX_PRIORITY,
						threadPriority));
			}

			this.minNumThreads = minNumThreads;
			this.maxNumThreads = maxNumThreads;
			this.threadPriority = threadPriority;
		}

		public int getMinNumThreads() {
			return minNumThreads;
		}

		public int getMaxNumThreads() {
			return maxNumThreads;
		}

		public int getThreadPriority() {
			return threadPriority;
		}

		@Override
		public Config getAkkaConfig() {
			return AkkaUtils.getThreadPoolExecutorConfig(this);
		}
	}

	/**
	 * Get dynamic properties based on two Flink configurations. If base config does not contain and target config
	 * contains the key or the value is different, it should be added to results. Otherwise, if the base config contains
	 * and target config does not contain the key, it will be ignored.
	 *
	 * @param baseConfig The base configuration.
	 * @param targetConfig The target configuration.
	 * @return Dynamic properties as string, separated by whitespace.
	 */
	public static String getDynamicPropertiesAsString(Configuration baseConfig, Configuration targetConfig) {

		String[] newAddedConfigs = targetConfig.keySet().stream().flatMap(
			(String key) -> {
				final String baseValue = baseConfig.getString(ConfigOptions.key(key).stringType().noDefaultValue());
				final String targetValue = targetConfig.getString(ConfigOptions.key(key).stringType().noDefaultValue());

				if (!baseConfig.keySet().contains(key) || !baseValue.equals(targetValue)) {
					return Stream.of("-" + CommandLineOptions.DYNAMIC_PROPERTY_OPTION.getOpt() + key +
						CommandLineOptions.DYNAMIC_PROPERTY_OPTION.getValueSeparator() + escapeForDifferentOS(targetValue));
				} else {
					return Stream.empty();
				}
			})
			.toArray(String[]::new);
		return String.join(" ", newAddedConfigs);
	}

	/**
	 * Escape all the dynamic property values.
	 * For Unix-like OS(Linux, MacOS, FREE_BSD, etc.), each value will be surrounded with single quotes. This works for
	 * all chars except single quote itself. To escape the single quote, close the quoting before it, insert the escaped
	 * single quote, and then re-open the quoting. For example, the value is foo'bar and the escaped value is
	 * 'foo'\''bar'. See <a href="https://stackoverflow.com/questions/15783701/which-characters-need-to-be-escaped-when-using-bash">https://stackoverflow.com/questions/15783701/which-characters-need-to-be-escaped-when-using-bash</a>
	 * for more information about Unix escaping.
	 *
	 * <p>For Windows OS, each value will be surrounded with double quotes. The double quote itself needs to be escaped with
	 * back slash. Also the caret symbol need to be escaped with double carets since Windows uses it to escape characters.
	 * See <a href="https://en.wikibooks.org/wiki/Windows_Batch_Scripting">https://en.wikibooks.org/wiki/Windows_Batch_Scripting</a>
	 * for more information about Windows escaping.
	 *
	 * @param value value to be escaped
	 * @return escaped value
	 */
	public static String escapeForDifferentOS(String value) {
		if (OperatingSystem.isWindows()) {
			return escapeWithDoubleQuote(value);
		} else {
			return escapeWithSingleQuote(value);
		}
	}

	public static String escapeWithSingleQuote(String value) {
		return "'" + UNIX_SINGLE_QUOTE_ESCAPER.escape(value) + "'";
	}

	public static String escapeWithDoubleQuote(String value) {
		return "\"" + WINDOWS_DOUBLE_QUOTE_ESCAPER.escape(value) + "\"";
	}
}
