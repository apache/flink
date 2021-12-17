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

import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

import java.util.LinkedHashMap;
import java.util.Map;

/** Utility class that contains all strings for CLI commands and messages. */
public final class CliStrings {

    private CliStrings() {
        // private
    }

    public static final String CLI_NAME = "Flink SQL CLI Client";
    public static final String DEFAULT_MARGIN = " ";

    // --------------------------------------------------------------------------------------------

    private static final String CMD_DESC_DELIMITER = "\t\t";

    /** SQL Client HELP command helper class. */
    private static final class SQLCliCommandsDescriptions {
        private int commandMaxLength;
        private final Map<String, String> commandsDescriptions;

        public SQLCliCommandsDescriptions() {
            this.commandsDescriptions = new LinkedHashMap<>();
            this.commandMaxLength = -1;
        }

        public SQLCliCommandsDescriptions commandDescription(String command, String description) {
            Preconditions.checkState(
                    StringUtils.isNotBlank(command), "content of command must not be empty.");
            Preconditions.checkState(
                    StringUtils.isNotBlank(description),
                    "content of command's description must not be empty.");
            this.updateMaxCommandLength(command.length());
            this.commandsDescriptions.put(command, description);
            return this;
        }

        private void updateMaxCommandLength(int newLength) {
            Preconditions.checkState(newLength > 0);
            if (this.commandMaxLength < newLength) {
                this.commandMaxLength = newLength;
            }
        }

        public AttributedString build() {
            AttributedStringBuilder attributedStringBuilder = new AttributedStringBuilder();
            if (!this.commandsDescriptions.isEmpty()) {
                this.commandsDescriptions.forEach(
                        (cmd, cmdDesc) -> {
                            attributedStringBuilder
                                    .style(AttributedStyle.DEFAULT.bold())
                                    .append(
                                            String.format(
                                                    String.format("%%-%ds", commandMaxLength), cmd))
                                    .append(CMD_DESC_DELIMITER)
                                    .style(AttributedStyle.DEFAULT)
                                    .append(cmdDesc)
                                    .append('\n');
                        });
            }
            return attributedStringBuilder.toAttributedString();
        }
    }

    private static final AttributedString SQL_CLI_COMMANDS_DESCRIPTIONS =
            new SQLCliCommandsDescriptions()
                    .commandDescription("HELP", "Prints the available commands.")
                    .commandDescription("QUIT/EXIT", "Quits the SQL CLI client.")
                    .commandDescription("CLEAR", "Clears the current terminal.")
                    .commandDescription(
                            "SET",
                            "Sets a session configuration property. Syntax: \"SET '<key>'='<value>';\". Use \"SET;\" for listing all properties.")
                    .commandDescription(
                            "RESET",
                            "Resets a session configuration property. Syntax: \"RESET '<key>';\". Use \"RESET;\" for reset all session properties.")
                    .commandDescription(
                            "INSERT INTO",
                            "Inserts the results of a SQL SELECT query into a declared table sink.")
                    .commandDescription(
                            "INSERT OVERWRITE",
                            "Inserts the results of a SQL SELECT query into a declared table sink and overwrite existing data.")
                    .commandDescription(
                            "SELECT", "Executes a SQL SELECT query on the Flink cluster.")
                    .commandDescription(
                            "EXPLAIN",
                            "Describes the execution plan of a query or table with the given name.")
                    .commandDescription(
                            "BEGIN STATEMENT SET",
                            "Begins a statement set. Syntax: \"BEGIN STATEMENT SET;\"")
                    .commandDescription("END", "Ends a statement set. Syntax: \"END;\"")
                    .commandDescription(
                            "ADD JAR",
                            "Adds the specified jar file to the submitted jobs' classloader. Syntax: \"ADD JAR '<path_to_filename>.jar'\"")
                    .commandDescription(
                            "REMOVE JAR",
                            "Removes the specified jar file from the submitted jobs' classloader. Syntax: \"REMOVE JAR '<path_to_filename>.jar'\"")
                    .commandDescription(
                            "SHOW JARS",
                            "Shows the list of user-specified jar dependencies. This list is impacted by the --jar and --library startup options as well as the ADD/REMOVE JAR commands.")
                    .build();

    // --------------------------------------------------------------------------------------------

    public static final AttributedString MESSAGE_HELP =
            new AttributedStringBuilder()
                    .append("The following commands are available:\n\n")
                    .append(SQL_CLI_COMMANDS_DESCRIPTIONS)
                    .style(AttributedStyle.DEFAULT.underline())
                    .append("\nHint")
                    .style(AttributedStyle.DEFAULT)
                    .append(
                            ": Make sure that a statement ends with \";\" for finalizing (multi-line) statements.")
                    // About Documentation Link.
                    .style(AttributedStyle.DEFAULT)
                    .append(
                            "\nYou can also type any Flink SQL statement, please visit https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/overview/ for more details.")
                    .toAttributedString();

    public static final String MESSAGE_WELCOME;

    // make findbugs happy
    static {
        MESSAGE_WELCOME =
                "                                   \u2592\u2593\u2588\u2588\u2593\u2588\u2588\u2592\n"
                        + "                               \u2593\u2588\u2588\u2588\u2588\u2592\u2592\u2588\u2593\u2592\u2593\u2588\u2588\u2588\u2593\u2592\n"
                        + "                            \u2593\u2588\u2588\u2588\u2593\u2591\u2591        \u2592\u2592\u2592\u2593\u2588\u2588\u2592  \u2592\n"
                        + "                          \u2591\u2588\u2588\u2592   \u2592\u2592\u2593\u2593\u2588\u2593\u2593\u2592\u2591      \u2592\u2588\u2588\u2588\u2588\n"
                        + "                          \u2588\u2588\u2592         \u2591\u2592\u2593\u2588\u2588\u2588\u2592    \u2592\u2588\u2592\u2588\u2592\n"
                        + "                            \u2591\u2593\u2588            \u2588\u2588\u2588   \u2593\u2591\u2592\u2588\u2588\n"
                        + "                              \u2593\u2588       \u2592\u2592\u2592\u2592\u2592\u2593\u2588\u2588\u2593\u2591\u2592\u2591\u2593\u2593\u2588\n"
                        + "                            \u2588\u2591 \u2588   \u2592\u2592\u2591       \u2588\u2588\u2588\u2593\u2593\u2588 \u2592\u2588\u2592\u2592\u2592\n"
                        + "                            \u2588\u2588\u2588\u2588\u2591   \u2592\u2593\u2588\u2593      \u2588\u2588\u2592\u2592\u2592 \u2593\u2588\u2588\u2588\u2592\n"
                        + "                         \u2591\u2592\u2588\u2593\u2593\u2588\u2588       \u2593\u2588\u2592    \u2593\u2588\u2592\u2593\u2588\u2588\u2593 \u2591\u2588\u2591\n"
                        + "                   \u2593\u2591\u2592\u2593\u2588\u2588\u2588\u2588\u2592 \u2588\u2588         \u2592\u2588    \u2588\u2593\u2591\u2592\u2588\u2592\u2591\u2592\u2588\u2592\n"
                        + "                  \u2588\u2588\u2588\u2593\u2591\u2588\u2588\u2593  \u2593\u2588           \u2588   \u2588\u2593 \u2592\u2593\u2588\u2593\u2593\u2588\u2592\n"
                        + "                \u2591\u2588\u2588\u2593  \u2591\u2588\u2591            \u2588  \u2588\u2592 \u2592\u2588\u2588\u2588\u2588\u2588\u2593\u2592 \u2588\u2588\u2593\u2591\u2592\n"
                        + "               \u2588\u2588\u2588\u2591 \u2591 \u2588\u2591          \u2593 \u2591\u2588 \u2588\u2588\u2588\u2588\u2588\u2592\u2591\u2591    \u2591\u2588\u2591\u2593  \u2593\u2591\n"
                        + "              \u2588\u2588\u2593\u2588 \u2592\u2592\u2593\u2592          \u2593\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2593\u2591       \u2592\u2588\u2592 \u2592\u2593 \u2593\u2588\u2588\u2593\n"
                        + "           \u2592\u2588\u2588\u2593 \u2593\u2588 \u2588\u2593\u2588       \u2591\u2592\u2588\u2588\u2588\u2588\u2588\u2593\u2593\u2592\u2591         \u2588\u2588\u2592\u2592  \u2588 \u2592  \u2593\u2588\u2592\n"
                        + "           \u2593\u2588\u2593  \u2593\u2588 \u2588\u2588\u2593 \u2591\u2593\u2593\u2593\u2593\u2593\u2593\u2593\u2592              \u2592\u2588\u2588\u2593           \u2591\u2588\u2592\n"
                        + "           \u2593\u2588    \u2588 \u2593\u2588\u2588\u2588\u2593\u2592\u2591              \u2591\u2593\u2593\u2593\u2588\u2588\u2588\u2593          \u2591\u2592\u2591 \u2593\u2588\n"
                        + "           \u2588\u2588\u2593    \u2588\u2588\u2592    \u2591\u2592\u2593\u2593\u2588\u2588\u2588\u2593\u2593\u2593\u2593\u2593\u2588\u2588\u2588\u2588\u2588\u2588\u2593\u2592            \u2593\u2588\u2588\u2588  \u2588\n"
                        + "          \u2593\u2588\u2588\u2588\u2592 \u2588\u2588\u2588   \u2591\u2593\u2593\u2592\u2591\u2591   \u2591\u2593\u2588\u2588\u2588\u2588\u2593\u2591                  \u2591\u2592\u2593\u2592  \u2588\u2593\n"
                        + "          \u2588\u2593\u2592\u2592\u2593\u2593\u2588\u2588  \u2591\u2592\u2592\u2591\u2591\u2591\u2592\u2592\u2592\u2592\u2593\u2588\u2588\u2593\u2591                            \u2588\u2593\n"
                        + "          \u2588\u2588 \u2593\u2591\u2592\u2588   \u2593\u2593\u2593\u2593\u2592\u2591\u2591  \u2592\u2588\u2593       \u2592\u2593\u2593\u2588\u2588\u2593    \u2593\u2592          \u2592\u2592\u2593\n"
                        + "          \u2593\u2588\u2593 \u2593\u2592\u2588  \u2588\u2593\u2591  \u2591\u2592\u2593\u2593\u2588\u2588\u2592            \u2591\u2593\u2588\u2592   \u2592\u2592\u2592\u2591\u2592\u2592\u2593\u2588\u2588\u2588\u2588\u2588\u2592\n"
                        + "           \u2588\u2588\u2591 \u2593\u2588\u2592\u2588\u2592  \u2592\u2593\u2593\u2592  \u2593\u2588                \u2588\u2591      \u2591\u2591\u2591\u2591   \u2591\u2588\u2592\n"
                        + "           \u2593\u2588   \u2592\u2588\u2593   \u2591     \u2588\u2591                \u2592\u2588              \u2588\u2593\n"
                        + "            \u2588\u2593   \u2588\u2588         \u2588\u2591                 \u2593\u2593        \u2592\u2588\u2593\u2593\u2593\u2592\u2588\u2591\n"
                        + "             \u2588\u2593 \u2591\u2593\u2588\u2588\u2591       \u2593\u2592                  \u2593\u2588\u2593\u2592\u2591\u2591\u2591\u2592\u2593\u2588\u2591    \u2592\u2588\n"
                        + "              \u2588\u2588   \u2593\u2588\u2593\u2591      \u2592                    \u2591\u2592\u2588\u2592\u2588\u2588\u2592      \u2593\u2593\n"
                        + "               \u2593\u2588\u2592   \u2592\u2588\u2593\u2592\u2591                         \u2592\u2592 \u2588\u2592\u2588\u2593\u2592\u2592\u2591\u2591\u2592\u2588\u2588\n"
                        + "                \u2591\u2588\u2588\u2592    \u2592\u2593\u2593\u2592                     \u2593\u2588\u2588\u2593\u2592\u2588\u2592 \u2591\u2593\u2593\u2593\u2593\u2592\u2588\u2593\n"
                        + "                  \u2591\u2593\u2588\u2588\u2592                          \u2593\u2591  \u2592\u2588\u2593\u2588  \u2591\u2591\u2592\u2592\u2592\n"
                        + "                      \u2592\u2593\u2593\u2593\u2593\u2593\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2591\u2591\u2593\u2593  \u2593\u2591\u2592\u2588\u2591\n"
                        + "          \n"
                        + "    ______ _ _       _       _____  ____  _         _____ _ _            _  BETA   \n"
                        + "   |  ____| (_)     | |     / ____|/ __ \\| |       / ____| (_)          | |  \n"
                        + "   | |__  | |_ _ __ | | __ | (___ | |  | | |      | |    | |_  ___ _ __ | |_ \n"
                        + "   |  __| | | | '_ \\| |/ /  \\___ \\| |  | | |      | |    | | |/ _ \\ '_ \\| __|\n"
                        + "   | |    | | | | | |   <   ____) | |__| | |____  | |____| | |  __/ | | | |_ \n"
                        + "   |_|    |_|_|_| |_|_|\\_\\ |_____/ \\___\\_\\______|  \\_____|_|_|\\___|_| |_|\\__|\n"
                        + "          \n"
                        + "        Welcome! Enter 'HELP;' to list all available commands. 'QUIT;' to exit.\n\n";
    }

    public static final String MESSAGE_QUIT = "Exiting " + CliStrings.CLI_NAME + "...";

    public static final String MESSAGE_SQL_EXECUTION_ERROR = "Could not execute SQL statement.";

    public static final String MESSAGE_STATEMENT_SET_SQL_EXECUTION_ERROR =
            "Only INSERT statement is allowed in Statement Set.";

    public static final String MESSAGE_STATEMENT_SET_END_CALL_ERROR =
            "No Statement Set to submit, \"END;\" command should be used after \"BEGIN STATEMENT SET;\".";

    public static final String MESSAGE_RESET =
            "All session properties have been set to their default values.";

    public static final String MESSAGE_RESET_KEY = "Session property has been reset.";

    public static final String MESSAGE_SET_KEY = "Session property has been set.";

    public static final String MESSAGE_REMOVED_KEY = "The specified key is not supported anymore.";

    public static final String MESSAGE_DEPRECATED_KEY =
            "The specified key '%s' is deprecated. Please use '%s' instead.";

    public static final String MESSAGE_EMPTY = "Result was empty.";

    public static final String MESSAGE_RESULT_QUIT = "Result retrieval cancelled.";

    public static final String MESSAGE_SUBMITTING_STATEMENT =
            "Submitting SQL update statement to the cluster...";

    public static final String MESSAGE_FINISH_STATEMENT =
            "Complete execution of the SQL update statement.";

    public static final String MESSAGE_STATEMENT_SUBMITTED =
            "SQL update statement has been successfully submitted to the cluster:";

    public static final String MESSAGE_BEGIN_STATEMENT_SET = "Begin a statement set.";

    public static final String MESSAGE_NO_STATEMENT_IN_STATEMENT_SET =
            "No statement in the statement set, skip submit.";

    public static final String MESSAGE_ADD_STATEMENT_TO_STATEMENT_SET =
            "Add SQL update statement to the statement set.";

    public static final String MESSAGE_EXECUTE_FILE = "Executing SQL from file.";

    public static final String MESSAGE_WAIT_EXECUTE =
            "Execute statement in sync mode. Please wait for the execution finish...";

    public static final String MESSAGE_EXECUTE_STATEMENT = "Execute statement succeed.";

    public static final String MESSAGE_ADD_JAR_STATEMENT =
            "The specified jar is added into session classloader.";

    public static final String MESSAGE_REMOVE_JAR_STATEMENT =
            "The specified jar is removed from session classloader.";

    // --------------------------------------------------------------------------------------------

    public static final String RESULT_TITLE = "SQL Query Result";

    public static final String RESULT_REFRESH_INTERVAL = "Refresh:";

    public static final String RESULT_PAGE = "Page:";

    public static final String RESULT_PAGE_OF = " of ";

    public static final String RESULT_LAST_REFRESH = "Updated:";

    public static final String RESULT_LAST_PAGE = "Last";

    public static final String RESULT_QUIT = "Quit";

    public static final String RESULT_REFRESH = "Refresh";

    public static final String RESULT_GOTO = "Goto Page";

    public static final String RESULT_NEXT = "Next Page";

    public static final String RESULT_PREV = "Prev Page";

    public static final String RESULT_LAST = "Last Page";

    public static final String RESULT_FIRST = "First Page";

    public static final String RESULT_SEARCH = "Search";

    public static final String RESULT_INC_REFRESH =
            "Inc Refresh"; // implementation assumes max length of 11

    public static final String RESULT_DEC_REFRESH = "Dec Refresh";

    public static final String RESULT_OPEN = "Open Row";

    public static final String RESULT_CHANGELOG = "Changelog";

    public static final String RESULT_TABLE = "Table";

    public static final String RESULT_STOPPED = "Table program finished.";

    public static final String RESULT_REFRESH_UNKNOWN = "Unknown";

    // --------------------------------------------------------------------------------------------

    public static final String INPUT_TITLE = "Input Dialog";

    public static final AttributedString INPUT_HELP =
            new AttributedStringBuilder()
                    .append("Press ")
                    .style(AttributedStyle.DEFAULT.inverse())
                    .append("Enter")
                    .style(AttributedStyle.DEFAULT)
                    .append(" to submit. Press ")
                    .style(AttributedStyle.DEFAULT.inverse())
                    .append("ESC")
                    .style(AttributedStyle.DEFAULT)
                    .append(" or submit an empty string to cancel.")
                    .toAttributedString();

    public static final String INPUT_ENTER_PAGE = "Enter page number:";

    public static final String INPUT_ERROR = "The input is invalid please check it again.";

    // --------------------------------------------------------------------------------------------

    public static final AttributedString ROW_QUIT =
            new AttributedStringBuilder()
                    .append("Press ")
                    .style(AttributedStyle.DEFAULT.inverse())
                    .append("Q")
                    .style(AttributedStyle.DEFAULT)
                    .append(" to go back.")
                    .toAttributedString();

    public static final String ROW_HEADER = "Row Summary";

    // --------------------------------------------------------------------------------------------

    public static AttributedString messageInfo(String message) {
        return new AttributedStringBuilder()
                .style(AttributedStyle.DEFAULT.bold().foreground(AttributedStyle.BLUE))
                .append("[INFO] ")
                .append(message)
                .toAttributedString();
    }

    public static AttributedString messageWarning(String message) {
        return new AttributedStringBuilder()
                .style(AttributedStyle.DEFAULT.bold().foreground(AttributedStyle.YELLOW))
                .append("[WARNING] ")
                .append(message)
                .toAttributedString();
    }

    public static AttributedString messageError(String message, Throwable t, boolean isVerbose) {
        while (t.getCause() != null
                && t.getCause().getMessage() != null
                && !t.getCause().getMessage().isEmpty()) {
            t = t.getCause();
        }
        if (isVerbose) {
            return messageError(message, ExceptionUtils.stringifyException(t));
        } else {
            return messageError(message, t.getClass().getName() + ": " + t.getMessage());
        }
    }

    public static AttributedString messageError(String message) {
        return messageError(message, (String) null);
    }

    public static AttributedString messageError(String message, String s) {
        final AttributedStringBuilder builder =
                new AttributedStringBuilder()
                        .style(AttributedStyle.DEFAULT.bold().foreground(AttributedStyle.RED))
                        .append("[ERROR] ")
                        .append(message);

        if (s != null) {
            builder.append(" Reason:\n").append(s);
        }

        return builder.toAttributedString();
    }
}
