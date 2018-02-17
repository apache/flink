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

package org.apache.flink.table.client.cli;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.client.SqlClientException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Parser for command line options.
 */
public class CliOptionsParser {

	public static final Option OPTION_HELP = Option
			.builder("h")
			.required(false)
			.longOpt("help")
			.desc(
				"Show the help message with descriptions of all options.")
			.build();

	public static final Option OPTION_SESSION = Option
			.builder("s")
			.required(false)
			.longOpt("session")
			.numberOfArgs(1)
			.argName("session identifier")
			.desc(
				"The identifier for a session. 'default' is the default identifier.")
			.build();

	public static final Option OPTION_ENVIRONMENT = Option
			.builder("e")
			.required(false)
			.longOpt("environment")
			.numberOfArgs(1)
			.argName("environment file")
			.desc(
				"The environment properties to be imported into the session. " +
				"It might overwrite default environment properties.")
			.build();

	public static final Option OPTION_DEFAULTS = Option
			.builder("d")
			.required(false)
			.longOpt("defaults")
			.numberOfArgs(1)
			.argName("environment file")
			.desc(
				"The environment properties with which every new session is initialized. " +
				"Properties might be overwritten by session properties.")
			.build();

	public static final Option OPTION_JAR = Option
			.builder("j")
			.required(false)
			.longOpt("jar")
			.numberOfArgs(1)
			.argName("JAR file")
			.desc(
				"A JAR file to be imported into the session. The file might contain " +
				"user-defined classes needed for the execution of statements such as " +
				"functions, table sources, or sinks. Can be used multiple times.")
			.build();

	public static final Option OPTION_LIBRARY = Option
			.builder("l")
			.required(false)
			.longOpt("library")
			.numberOfArgs(1)
			.argName("JAR directory")
			.desc(
				"A JAR file directory with which every new session is initialized. The files might " +
				"contain user-defined classes needed for the execution of statements such as " +
				"functions, table sources, or sinks. Can be used multiple times.")
			.build();

	private static final Options EMBEDDED_MODE_CLIENT_OPTIONS = getEmbeddedModeClientOptions(new Options());
	private static final Options GATEWAY_MODE_CLIENT_OPTIONS = getGatewayModeClientOptions(new Options());
	private static final Options GATEWAY_MODE_GATEWAY_OPTIONS = getGatewayModeGatewayOptions(new Options());

	private static void buildGeneralOptions(Options options) {
		options.addOption(OPTION_HELP);
	}

	public static Options getEmbeddedModeClientOptions(Options options) {
		buildGeneralOptions(options);
		options.addOption(OPTION_SESSION);
		options.addOption(OPTION_ENVIRONMENT);
		options.addOption(OPTION_DEFAULTS);
		options.addOption(OPTION_JAR);
		options.addOption(OPTION_LIBRARY);
		return options;
	}

	public static Options getGatewayModeClientOptions(Options options) {
		buildGeneralOptions(options);
		options.addOption(OPTION_SESSION);
		options.addOption(OPTION_ENVIRONMENT);
		return options;
	}

	public static Options getGatewayModeGatewayOptions(Options options) {
		buildGeneralOptions(options);
		options.addOption(OPTION_DEFAULTS);
		options.addOption(OPTION_JAR);
		options.addOption(OPTION_LIBRARY);
		return options;
	}

	// --------------------------------------------------------------------------------------------
	//  Help
	// --------------------------------------------------------------------------------------------

	/**
	 * Prints the help for the client.
	 */
	public static void printHelpClient() {
		System.out.println("./sql-client [MODE] [OPTIONS]");
		System.out.println();
		System.out.println("The following options are available:");

		printHelpEmbeddedModeClient();
		printHelpGatewayModeClient();

		System.out.println();
	}

	/**
	 * Prints the help for the gateway.
	 */
	public static void printHelpGateway() {
		System.out.println("./sql-gateway [OPTIONS]");
		System.out.println();
		System.out.println("The following options are available:");

		printHelpGatewayModeGateway();

		System.out.println();
	}

	public static void printHelpEmbeddedModeClient() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);

		System.out.println("\nMode \"embedded\" submits Flink jobs from the local machine.");
		System.out.println("\n  Syntax: embedded [OPTIONS]");
		formatter.setSyntaxPrefix("  \"embedded\" mode options:");
		formatter.printHelp(" ", EMBEDDED_MODE_CLIENT_OPTIONS);

		System.out.println();
	}

	public static void printHelpGatewayModeClient() {
// TODO enable this once gateway mode is in place
//		HelpFormatter formatter = new HelpFormatter();
//		formatter.setLeftPadding(5);
//		formatter.setWidth(80);
//
//		System.out.println("\nIn future versions: Mode \"gateway\" mode connects to the SQL gateway for submission.");
//		System.out.println("\n  Syntax: gateway [OPTIONS]");
//		formatter.setSyntaxPrefix("  \"gateway\" mode options:");
//		formatter.printHelp(" ", GATEWAY_MODE_CLIENT_OPTIONS);
//
//		System.out.println();
	}

	public static void printHelpGatewayModeGateway() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);

		formatter.printHelp(" ", GATEWAY_MODE_GATEWAY_OPTIONS);

		System.out.println();
	}

	// --------------------------------------------------------------------------------------------
	//  Line Parsing
	// --------------------------------------------------------------------------------------------

	public static CliOptions parseEmbeddedModeClient(String[] args) {
		try {
			DefaultParser parser = new DefaultParser();
			CommandLine line = parser.parse(EMBEDDED_MODE_CLIENT_OPTIONS, args, true);
			return new CliOptions(
				line.hasOption(CliOptionsParser.OPTION_HELP.getOpt()),
				checkSessionId(line),
				checkUrl(line, CliOptionsParser.OPTION_ENVIRONMENT),
				checkUrl(line, CliOptionsParser.OPTION_DEFAULTS),
				checkUrls(line, CliOptionsParser.OPTION_JAR),
				checkUrls(line, CliOptionsParser.OPTION_LIBRARY)
			);
		}
		catch (ParseException e) {
			throw new SqlClientException(e.getMessage());
		}
	}

	public static CliOptions parseGatewayModeClient(String[] args) {
		try {
			DefaultParser parser = new DefaultParser();
			CommandLine line = parser.parse(GATEWAY_MODE_CLIENT_OPTIONS, args, true);
			return new CliOptions(
				line.hasOption(CliOptionsParser.OPTION_HELP.getOpt()),
				checkSessionId(line),
				checkUrl(line, CliOptionsParser.OPTION_ENVIRONMENT),
				null,
				checkUrls(line, CliOptionsParser.OPTION_JAR),
				checkUrls(line, CliOptionsParser.OPTION_LIBRARY)
			);
		}
		catch (ParseException e) {
			throw new SqlClientException(e.getMessage());
		}
	}

	public static CliOptions parseGatewayModeGateway(String[] args) {
		try {
			DefaultParser parser = new DefaultParser();
			CommandLine line = parser.parse(GATEWAY_MODE_GATEWAY_OPTIONS, args, true);
			return new CliOptions(
				line.hasOption(CliOptionsParser.OPTION_HELP.getOpt()),
				null,
				null,
				checkUrl(line, CliOptionsParser.OPTION_DEFAULTS),
				checkUrls(line, CliOptionsParser.OPTION_JAR),
				checkUrls(line, CliOptionsParser.OPTION_LIBRARY)
			);
		}
		catch (ParseException e) {
			throw new SqlClientException(e.getMessage());
		}
	}

	// --------------------------------------------------------------------------------------------

	private static URL checkUrl(CommandLine line, Option option) {
		final List<URL> urls = checkUrls(line, option);
		if (urls != null && !urls.isEmpty()) {
			return urls.get(0);
		}
		return null;
	}

	private static List<URL> checkUrls(CommandLine line, Option option) {
		if (line.hasOption(option.getOpt())) {
			final String[] urls = line.getOptionValues(option.getOpt());
			return Arrays.stream(urls)
				.distinct()
				.map((url) -> {
					try {
						return Path.fromLocalFile(new File(url).getAbsoluteFile()).toUri().toURL();
					} catch (Exception e) {
						throw new SqlClientException("Invalid path for option '" + option.getLongOpt() + "': " + url, e);
					}
				})
				.collect(Collectors.toList());
		}
		return null;
	}

	private static String checkSessionId(CommandLine line) {
		final String sessionId = line.getOptionValue(CliOptionsParser.OPTION_SESSION.getOpt());
		if (sessionId != null && !sessionId.matches("[a-zA-Z0-9_\\-.]+")) {
			throw new SqlClientException("Session identifier must only consists of 'a-zA-Z0-9_-.'.");
		}
		return sessionId;
	}
}
