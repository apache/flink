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

import org.apache.flink.table.client.cli.SqlCommandParser.SqlCommand;

import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

/** Utility class that contains all strings for CLI commands and messages. */
public final class CliStrings {

    private CliStrings() {
        // private
    }

    public static final String CLI_NAME = "Flink SQL CLI Client";
    public static final String DEFAULT_MARGIN = " ";
    public static final String NULL_COLUMN = "(NULL)";

    // --------------------------------------------------------------------------------------------

    public static final AttributedString MESSAGE_HELP =
            new AttributedStringBuilder()
                    .append("The following commands are available:\n\n")
                    .append(formatCommand(SqlCommand.CLEAR, "Clears the current terminal."))
                    .append(
                            formatCommand(
                                    SqlCommand.CREATE_TABLE,
                                    "Create table under current catalog and database."))
                    .append(
                            formatCommand(
                                    SqlCommand.DROP_TABLE,
                                    "Drop table with optional catalog and database. Syntax: 'DROP TABLE [IF EXISTS] <name>;'"))
                    .append(
                            formatCommand(
                                    SqlCommand.CREATE_VIEW,
                                    "Creates a virtual table from a SQL query. Syntax: 'CREATE VIEW <name> AS <query>;'"))
                    .append(
                            formatCommand(
                                    SqlCommand.DESCRIBE,
                                    "Describes the schema of a table with the given name."))
                    .append(
                            formatCommand(
                                    SqlCommand.DROP_VIEW,
                                    "Deletes a previously created virtual table. Syntax: 'DROP VIEW <name>;'"))
                    .append(
                            formatCommand(
                                    SqlCommand.EXPLAIN,
                                    "Describes the execution plan of a query or table with the given name."))
                    .append(formatCommand(SqlCommand.HELP, "Prints the available commands."))
                    .append(
                            formatCommand(
                                    SqlCommand.INSERT_INTO,
                                    "Inserts the results of a SQL SELECT query into a declared table sink."))
                    .append(
                            formatCommand(
                                    SqlCommand.INSERT_OVERWRITE,
                                    "Inserts the results of a SQL SELECT query into a declared table sink and overwrite existing data."))
                    .append(formatCommand(SqlCommand.QUIT, "Quits the SQL CLI client."))
                    .append(
                            formatCommand(
                                    SqlCommand.RESET,
                                    "Resets all session configuration properties."))
                    .append(
                            formatCommand(
                                    SqlCommand.SELECT,
                                    "Executes a SQL SELECT query on the Flink cluster."))
                    .append(
                            formatCommand(
                                    SqlCommand.SET,
                                    "Sets a session configuration property. Syntax: 'SET <key>=<value>;'. Use 'SET;' for listing all properties."))
                    .append(
                            formatCommand(
                                    SqlCommand.SHOW_FUNCTIONS,
                                    "Shows all user-defined and built-in functions."))
                    .append(formatCommand(SqlCommand.SHOW_TABLES, "Shows all registered tables."))
                    .append(
                            formatCommand(
                                    SqlCommand.SOURCE,
                                    "Reads a SQL SELECT query from a file and executes it on the Flink cluster."))
                    .append(
                            formatCommand(
                                    SqlCommand.USE_CATALOG,
                                    "Sets the current catalog. The current database is set to the catalog's default one. Experimental! Syntax: 'USE CATALOG <name>;'"))
                    .append(
                            formatCommand(
                                    SqlCommand.USE,
                                    "Sets the current default database. Experimental! Syntax: 'USE <name>;'"))
                    .style(AttributedStyle.DEFAULT.underline())
                    .append("\nHint")
                    .style(AttributedStyle.DEFAULT)
                    .append(
                            ": Make sure that a statement ends with ';' for finalizing (multi-line) statements.")
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

    public static final String MESSAGE_RESET =
            "All session properties have been set to their default values.";

    public static final String MESSAGE_SET = "Session property has been set.";

    public static final String MESSAGE_EMPTY = "Result was empty.";

    public static final String MESSAGE_RESULT_QUIT = "Result retrieval cancelled.";

    public static final String MESSAGE_SUBMITTING_STATEMENT =
            "Submitting SQL update statement to the cluster...";

    public static final String MESSAGE_STATEMENT_SUBMITTED =
            "Table update statement has been successfully submitted to the cluster:";

    public static final String MESSAGE_MAX_SIZE_EXCEEDED =
            "The given file exceeds the maximum number of characters.";

    public static final String MESSAGE_WILL_EXECUTE = "Executing the following statement:";

    public static final String MESSAGE_UNSUPPORTED_SQL = "Unsupported SQL statement.";

    public static final String MESSAGE_TABLE_CREATED = "Table has been created.";

    public static final String MESSAGE_TABLE_REMOVED = "Table has been removed.";

    public static final String MESSAGE_ALTER_TABLE_SUCCEEDED = "Alter table succeeded!";

    public static final String MESSAGE_ALTER_TABLE_FAILED = "Alter table failed!";

    public static final String MESSAGE_VIEW_CREATED = "View has been created.";

    public static final String MESSAGE_VIEW_REMOVED = "View has been removed.";

    public static final String MESSAGE_ALTER_VIEW_SUCCEEDED = "Alter view succeeded!";

    public static final String MESSAGE_ALTER_VIEW_FAILED = "Alter view failed!";

    public static final String MESSAGE_FUNCTION_CREATED = "Function has been created.";

    public static final String MESSAGE_FUNCTION_REMOVED = "Function has been removed.";

    public static final String MESSAGE_ALTER_FUNCTION_SUCCEEDED = "Alter function succeeded!";

    public static final String MESSAGE_ALTER_FUNCTION_FAILED = "Alter function failed!";

    public static final String MESSAGE_DATABASE_CREATED = "Database has been created.";

    public static final String MESSAGE_DATABASE_REMOVED = "Database has been removed.";

    public static final String MESSAGE_ALTER_DATABASE_SUCCEEDED = "Alter database succeeded!";

    public static final String MESSAGE_ALTER_DATABASE_FAILED = "Alter database failed!";

    public static final String MESSAGE_CATALOG_CREATED = "Catalog has been created.";

    public static final String MESSAGE_CATALOG_REMOVED = "Catalog has been removed.";

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

    public static AttributedString messageError(String message, Throwable t) {
        while (t.getCause() != null
                && t.getCause().getMessage() != null
                && !t.getCause().getMessage().isEmpty()) {
            t = t.getCause();
        }
        return messageError(message, t.getClass().getName() + ": " + t.getMessage());
        // return messageError(message, ExceptionUtils.stringifyException(t));
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

    private static AttributedString formatCommand(SqlCommand cmd, String description) {
        return new AttributedStringBuilder()
                .style(AttributedStyle.DEFAULT.bold())
                .append(cmd.toString())
                .append("\t\t")
                .style(AttributedStyle.DEFAULT)
                .append(description)
                .append('\n')
                .toAttributedString();
    }
}
