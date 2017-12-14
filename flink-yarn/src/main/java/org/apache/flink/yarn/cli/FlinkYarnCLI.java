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

package org.apache.flink.yarn.cli;

import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.YarnClusterClientV2;
import org.apache.flink.yarn.YarnClusterDescriptorV2;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.client.cli.CliFrontendParser.ADDRESS_OPTION;

/**
 * Class handling the command line interface to the YARN per job mode under flip-6.
 */
public class FlinkYarnCLI implements CustomCommandLine<YarnClusterClientV2> {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkYarnCLI.class);

	/** The id for the CommandLine interface. */
	private static final String ID = "yarn";

	private static final String YARN_DYNAMIC_PROPERTIES_SEPARATOR = "@@"; // this has to be a regex for String.split()

	//------------------------------------ Command Line argument options -------------------------
	// the prefix transformation is used by the CliFrontend static constructor.
	private final Option queue;
	private final Option shipPath;
	private final Option flinkJar;
	private final Option jmMemory;
	private final Option detached;
	private final Option zookeeperNamespace;

	private final Options allOptions;

	/**
	 * Dynamic properties allow the user to specify additional configuration values with -D, such as
	 * <tt> -Dfs.overwrite-files=true  -Dtaskmanager.network.memory.min=536346624</tt>.
	 */
	private final Option dynamicProperties;

	//------------------------------------ Internal fields -------------------------
	// use detach mode as default
	private boolean detachedMode = true;

	public FlinkYarnCLI(String shortPrefix, String longPrefix) {

		queue = new Option(shortPrefix + "qu", longPrefix + "queue", true, "Specify YARN queue.");
		shipPath = new Option(shortPrefix + "t", longPrefix + "ship", true, "Ship files in the specified directory (t for transfer)");
		flinkJar = new Option(shortPrefix + "j", longPrefix + "jar", true, "Path to Flink jar file");
		jmMemory = new Option(shortPrefix + "jm", longPrefix + "jobManagerMemory", true, "Memory for JobManager Container [in MB]");
		dynamicProperties = new Option(shortPrefix + "D", true, "Dynamic properties");
		detached = new Option(shortPrefix + "a", longPrefix + "attached", false, "Start attached");
		zookeeperNamespace = new Option(shortPrefix + "z", longPrefix + "zookeeperNamespace", true, "Namespace to create the Zookeeper sub-paths for high availability mode");

		allOptions = new Options();
		allOptions.addOption(flinkJar);
		allOptions.addOption(jmMemory);
		allOptions.addOption(queue);
		allOptions.addOption(shipPath);
		allOptions.addOption(dynamicProperties);
		allOptions.addOption(detached);
		allOptions.addOption(zookeeperNamespace);
	}

	public YarnClusterDescriptorV2 createDescriptor(
			Configuration configuration,
			String defaultApplicationName,
			CommandLine cmd) {

		YarnClusterDescriptorV2 yarnClusterDescriptor = new YarnClusterDescriptorV2(configuration, CliFrontend.getConfigurationDirectoryFromEnv());

		// Jar Path
		Path localJarPath;
		if (cmd.hasOption(flinkJar.getOpt())) {
			String userPath = cmd.getOptionValue(flinkJar.getOpt());
			if (!userPath.startsWith("file://")) {
				userPath = "file://" + userPath;
			}
			localJarPath = new Path(userPath);
		} else {
			LOG.info("No path for the flink jar passed. Using the location of "
				+ yarnClusterDescriptor.getClass() + " to locate the jar");
			String encodedJarPath =
				yarnClusterDescriptor.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
			try {
				// we have to decode the url encoded parts of the path
				String decodedPath = URLDecoder.decode(encodedJarPath, Charset.defaultCharset().name());
				localJarPath = new Path(new File(decodedPath).toURI());
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException("Couldn't decode the encoded Flink dist jar path: " + encodedJarPath +
					" Please supply a path manually via the -" + flinkJar.getOpt() + " option.");
			}
		}

		yarnClusterDescriptor.setLocalJarPath(localJarPath);

		List<File> shipFiles = new ArrayList<>();
		// path to directory to ship
		if (cmd.hasOption(shipPath.getOpt())) {
			String shipPath = cmd.getOptionValue(this.shipPath.getOpt());
			File shipDir = new File(shipPath);
			if (shipDir.isDirectory()) {
				shipFiles.add(shipDir);
			} else {
				LOG.warn("Ship directory is not a directory. Ignoring it.");
			}
		}

		yarnClusterDescriptor.addShipFiles(shipFiles);

		// queue
		if (cmd.hasOption(queue.getOpt())) {
			yarnClusterDescriptor.setQueue(cmd.getOptionValue(queue.getOpt()));
		}

		String[] dynamicProperties = null;
		if (cmd.hasOption(this.dynamicProperties.getOpt())) {
			dynamicProperties = cmd.getOptionValues(this.dynamicProperties.getOpt());
		}
		String dynamicPropertiesEncoded = StringUtils.join(dynamicProperties, YARN_DYNAMIC_PROPERTIES_SEPARATOR);

		yarnClusterDescriptor.setDynamicPropertiesEncoded(dynamicPropertiesEncoded);

		if (cmd.hasOption(detached.getOpt()) || cmd.hasOption(CliFrontendParser.DETACHED_OPTION.getOpt())) {
			// TODO: not support non detach mode now.
			//this.detachedMode = false;
		}
		yarnClusterDescriptor.setDetachedMode(this.detachedMode);

		if (defaultApplicationName != null) {
			yarnClusterDescriptor.setName(defaultApplicationName);
		}

		if (cmd.hasOption(zookeeperNamespace.getOpt())) {
			String zookeeperNamespace = cmd.getOptionValue(this.zookeeperNamespace.getOpt());
			yarnClusterDescriptor.setZookeeperNamespace(zookeeperNamespace);
		}

		return yarnClusterDescriptor;
	}

	private void printUsage() {
		System.out.println("Usage:");
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(200);
		formatter.setLeftPadding(5);

		formatter.setSyntaxPrefix("   Optional");
		Options options = new Options();
		addGeneralOptions(options);
		addRunOptions(options);
		formatter.printHelp(" ", options);
	}

	@Override
	public boolean isActive(CommandLine commandLine, Configuration configuration) {
		String jobManagerOption = commandLine.getOptionValue(ADDRESS_OPTION.getOpt(), null);
		boolean yarnJobManager = ID.equals(jobManagerOption);
		return yarnJobManager;
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public void addRunOptions(Options baseOptions) {
		for (Object option : allOptions.getOptions()) {
			baseOptions.addOption((Option) option);
		}
	}

	@Override
	public void addGeneralOptions(Options baseOptions) {
	}

	@Override
	public YarnClusterClientV2 retrieveCluster(
			CommandLine cmdLine,
			Configuration config,
			String configurationDirectory) throws UnsupportedOperationException {

		throw new UnsupportedOperationException("Not support retrieveCluster since Flip-6.");
	}

	@Override
	public YarnClusterClientV2 createCluster(
			String applicationName,
			CommandLine cmdLine,
			Configuration config,
			String configurationDirectory,
			List<URL> userJarFiles) throws Exception {
		Preconditions.checkNotNull(userJarFiles, "User jar files should not be null.");

		YarnClusterDescriptorV2 yarnClusterDescriptor = createDescriptor(config, applicationName, cmdLine);
		yarnClusterDescriptor.setProvidedUserJarFiles(userJarFiles);

		return new YarnClusterClientV2(
			yarnClusterDescriptor,
			config);
	}

	private void logAndSysout(String message) {
		LOG.info(message);
		System.out.println(message);
	}

}
