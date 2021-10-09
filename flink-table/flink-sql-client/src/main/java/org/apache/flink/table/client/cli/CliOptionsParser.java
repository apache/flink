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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.client.SqlClientException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.client.cli.CliFrontendParser.PYARCHIVE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYCLIENTEXEC_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYEXEC_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYFILES_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYREQUIREMENTS_OPTION;

/** Parser for command line options. */
public class CliOptionsParser {

    public static final Option OPTION_HELP =
            Option.builder("h")
                    .required(false)
                    .longOpt("help")
                    .desc("Show the help message with descriptions of all options.")
                    .build();

    public static final Option OPTION_SESSION =
            Option.builder("s")
                    .required(false)
                    .longOpt("session")
                    .numberOfArgs(1)
                    .argName("session identifier")
                    .desc("The identifier for a session. 'default' is the default identifier.")
                    .build();

    public static final Option OPTION_INIT_FILE =
            Option.builder("i")
                    .required(false)
                    .longOpt("init")
                    .numberOfArgs(1)
                    .argName("initialization file")
                    .desc(
                            "Script file that used to init the session context. "
                                    + "If get error in execution, the sql client will exit. Notice it's not allowed to add query or insert into the init file.")
                    .build();

    public static final Option OPTION_FILE =
            Option.builder("f")
                    .required(false)
                    .longOpt("file")
                    .numberOfArgs(1)
                    .argName("script file")
                    .desc(
                            "Script file that should be executed. In this mode, "
                                    + "the client will not open an interactive terminal.")
                    .build();

    public static final Option OPTION_JAR =
            Option.builder("j")
                    .required(false)
                    .longOpt("jar")
                    .numberOfArgs(1)
                    .argName("JAR file")
                    .desc(
                            "A JAR file to be imported into the session. The file might contain "
                                    + "user-defined classes needed for the execution of statements such as "
                                    + "functions, table sources, or sinks. Can be used multiple times.")
                    .build();

    public static final Option OPTION_LIBRARY =
            Option.builder("l")
                    .required(false)
                    .longOpt("library")
                    .numberOfArgs(1)
                    .argName("JAR directory")
                    .desc(
                            "A JAR file directory with which every new session is initialized. The files might "
                                    + "contain user-defined classes needed for the execution of statements such as "
                                    + "functions, table sources, or sinks. Can be used multiple times.")
                    .build();

    @Deprecated
    public static final Option OPTION_UPDATE =
            Option.builder("u")
                    .required(false)
                    .longOpt("update")
                    .numberOfArgs(1)
                    .argName("SQL update statement")
                    .desc(
                            String.format(
                                    "Deprecated Experimental (for testing only!) feature: Instructs the SQL Client to immediately execute "
                                            + "the given update statement after starting up. The process is shut down after the "
                                            + "statement has been submitted to the cluster and returns an appropriate return code. "
                                            + "Currently, this feature is only supported for INSERT INTO statements that declare "
                                            + "the target sink table."
                                            + "Please use option -%s to submit update statement.",
                                    OPTION_FILE.getOpt()))
                    .build();

    public static final Option OPTION_HISTORY =
            Option.builder("hist")
                    .required(false)
                    .longOpt("history")
                    .numberOfArgs(1)
                    .argName("History file path")
                    .desc(
                            "The file which you want to save the command history into. If not specified, we will "
                                    + "auto-generate one under your user's home directory.")
                    .build();

    private static final Options EMBEDDED_MODE_CLIENT_OPTIONS =
            getEmbeddedModeClientOptions(new Options());
    private static final Options GATEWAY_MODE_CLIENT_OPTIONS =
            getGatewayModeClientOptions(new Options());
    private static final Options GATEWAY_MODE_GATEWAY_OPTIONS =
            getGatewayModeGatewayOptions(new Options());

    private static void buildGeneralOptions(Options options) {
        options.addOption(OPTION_HELP);
    }

    public static Options getEmbeddedModeClientOptions(Options options) {
        buildGeneralOptions(options);
        options.addOption(OPTION_SESSION);
        options.addOption(OPTION_INIT_FILE);
        options.addOption(OPTION_FILE);
        options.addOption(OPTION_JAR);
        options.addOption(OPTION_LIBRARY);
        options.addOption(OPTION_UPDATE);
        options.addOption(OPTION_HISTORY);
        options.addOption(PYFILES_OPTION);
        options.addOption(PYREQUIREMENTS_OPTION);
        options.addOption(PYARCHIVE_OPTION);
        options.addOption(PYEXEC_OPTION);
        options.addOption(PYCLIENTEXEC_OPTION);
        return options;
    }

    public static Options getGatewayModeClientOptions(Options options) {
        buildGeneralOptions(options);
        options.addOption(OPTION_SESSION);
        options.addOption(OPTION_UPDATE);
        options.addOption(OPTION_HISTORY);
        options.addOption(PYFILES_OPTION);
        options.addOption(PYREQUIREMENTS_OPTION);
        options.addOption(PYARCHIVE_OPTION);
        options.addOption(PYEXEC_OPTION);
        options.addOption(PYCLIENTEXEC_OPTION);
        return options;
    }

    public static Options getGatewayModeGatewayOptions(Options options) {
        buildGeneralOptions(options);
        options.addOption(OPTION_JAR);
        options.addOption(OPTION_LIBRARY);
        options.addOption(PYFILES_OPTION);
        options.addOption(PYREQUIREMENTS_OPTION);
        options.addOption(PYARCHIVE_OPTION);
        options.addOption(PYEXEC_OPTION);
        options.addOption(PYCLIENTEXEC_OPTION);
        return options;
    }

    // --------------------------------------------------------------------------------------------
    //  Help
    // --------------------------------------------------------------------------------------------

    /** Prints the help for the client. */
    public static void printHelpClient() {
        System.out.println("./sql-client [MODE] [OPTIONS]");
        System.out.println();
        System.out.println("The following options are available:");

        printHelpEmbeddedModeClient();
        printHelpGatewayModeClient();

        System.out.println();
    }

    /** Prints the help for the gateway. */
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

        System.out.println(
                "\nMode \"embedded\" (default) submits Flink jobs from the local machine.");
        System.out.println("\n  Syntax: [embedded] [OPTIONS]");
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
        //		System.out.println("\nIn future versions: Mode \"gateway\" mode connects to the SQL
        // gateway for submission.");
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
                    checkUrl(line, CliOptionsParser.OPTION_INIT_FILE),
                    checkUrl(line, CliOptionsParser.OPTION_FILE),
                    checkUrls(line, CliOptionsParser.OPTION_JAR),
                    checkUrls(line, CliOptionsParser.OPTION_LIBRARY),
                    line.getOptionValue(CliOptionsParser.OPTION_UPDATE.getOpt()),
                    line.getOptionValue(CliOptionsParser.OPTION_HISTORY.getOpt()),
                    getPythonConfiguration(line));
        } catch (ParseException e) {
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
                    null,
                    null,
                    checkUrls(line, CliOptionsParser.OPTION_JAR),
                    checkUrls(line, CliOptionsParser.OPTION_LIBRARY),
                    line.getOptionValue(CliOptionsParser.OPTION_UPDATE.getOpt()),
                    line.getOptionValue(CliOptionsParser.OPTION_HISTORY.getOpt()),
                    getPythonConfiguration(line));
        } catch (ParseException e) {
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
                    null,
                    checkUrls(line, CliOptionsParser.OPTION_JAR),
                    checkUrls(line, CliOptionsParser.OPTION_LIBRARY),
                    null,
                    null,
                    getPythonConfiguration(line));
        } catch (ParseException e) {
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
                    .map(
                            (url) -> {
                                checkFilePath(url);
                                try {
                                    return Path.fromLocalFile(new File(url).getAbsoluteFile())
                                            .toUri()
                                            .toURL();
                                } catch (Exception e) {
                                    throw new SqlClientException(
                                            "Invalid path for option '"
                                                    + option.getLongOpt()
                                                    + "': "
                                                    + url,
                                            e);
                                }
                            })
                    .collect(Collectors.toList());
        }
        return null;
    }

    public static void checkFilePath(String filePath) {
        Path path = new Path(filePath);
        String scheme = path.toUri().getScheme();
        if (scheme != null && !scheme.equals("file")) {
            throw new SqlClientException("SQL Client only supports to load files in local.");
        }
    }

    private static String checkSessionId(CommandLine line) {
        final String sessionId = line.getOptionValue(CliOptionsParser.OPTION_SESSION.getOpt());
        if (sessionId != null && !sessionId.matches("[a-zA-Z0-9_\\-.]+")) {
            throw new SqlClientException(
                    "Session identifier must only consists of 'a-zA-Z0-9_-.'.");
        }
        return sessionId;
    }

    private static Configuration getPythonConfiguration(CommandLine line) {
        try {
            Class<?> clazz =
                    Class.forName(
                            "org.apache.flink.python.util.PythonDependencyUtils",
                            true,
                            Thread.currentThread().getContextClassLoader());
            Method parsePythonDependencyConfiguration =
                    clazz.getMethod("parsePythonDependencyConfiguration", CommandLine.class);
            return (Configuration) parsePythonDependencyConfiguration.invoke(null, line);
        } catch (ClassNotFoundException
                | NoSuchMethodException
                | IllegalAccessException
                | InvocationTargetException e) {
            throw new SqlClientException("Failed to parse the Python command line options.", e);
        }
    }
}
