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

package org.apache.flink.table.gateway.cli;

import org.apache.flink.table.gateway.api.utils.SqlGatewayException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.PrintStream;
import java.io.PrintWriter;

/** Parser to parse the command line options. */
public class SqlGatewayOptionsParser {

    public static final Option OPTION_HELP =
            Option.builder("h")
                    .required(false)
                    .longOpt("help")
                    .desc("Show the help message with descriptions of all options.")
                    .build();

    public static final Option DYNAMIC_PROPERTY_OPTION =
            Option.builder("D")
                    .argName("property=value")
                    .numberOfArgs(2)
                    .valueSeparator('=')
                    .desc("Use value for given property")
                    .build();

    // --------------------------------------------------------------------------------------------
    //  Line Parsing
    // --------------------------------------------------------------------------------------------

    public static SqlGatewayOptions parseSqlGatewayOptions(String[] args) {
        try {
            DefaultParser parser = new DefaultParser();
            CommandLine line = parser.parse(getSqlGatewayOptions(), args, true);
            return new SqlGatewayOptions(
                    line.hasOption(SqlGatewayOptionsParser.OPTION_HELP.getOpt()),
                    line.getOptionProperties(DYNAMIC_PROPERTY_OPTION.getOpt()));
        } catch (ParseException e) {
            throw new SqlGatewayException(e.getMessage());
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Help
    // --------------------------------------------------------------------------------------------

    /** Prints the help for the client. */
    public static void printHelpSqlGateway(PrintStream writer) {
        writer.println();
        printHelpForStart(writer);
    }

    private static void printHelpForStart(PrintStream printStream) {
        PrintWriter writer = new PrintWriter(printStream);
        writer.println("Start the Flink SQL Gateway as a daemon to submit Flink SQL.");
        HelpFormatter formatter = new HelpFormatter();
        formatter.setLeftPadding(5);
        formatter.setWidth(80);

        writer.println("\n  Syntax: start [OPTIONS]");
        formatter.setSyntaxPrefix("  \"start\" options:");

        formatter.printOptions(
                writer,
                formatter.getWidth(),
                getSqlGatewayOptions(),
                formatter.getLeftPadding(),
                formatter.getDescPadding());

        writer.println();
        writer.flush();
    }

    // --------------------------------------------------------------------------------------------

    private static Options getSqlGatewayOptions() {
        Options options = new Options();
        options.addOption(OPTION_HELP);
        options.addOption(DYNAMIC_PROPERTY_OPTION);
        return options;
    }
}
