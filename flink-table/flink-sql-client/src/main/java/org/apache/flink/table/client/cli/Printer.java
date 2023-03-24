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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.utils.print.PrintStyle;

import org.jline.terminal.Terminal;
import org.jline.utils.InfoCmp;

import java.io.Closeable;

import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DML_SYNC;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_EXECUTE_STATEMENT;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_FINISH_STATEMENT;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_STATEMENT_SUBMITTED;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_SUBMITTING_STATEMENT;

/** Printer to print the results to the terminal. */
public interface Printer extends Closeable {

    /** Flag to determine whether to quit the process. */
    boolean isQuitCommand();

    /** Print the results to the terminal. */
    void print(Terminal terminal);

    /** Close the resource of the {@link Printer}. */
    @Override
    default void close() {}

    // --------------------------------------------------------------------------------------------

    static ClearCommandPrinter createClearCommandPrinter() {
        return ClearCommandPrinter.INSTANCE;
    }

    static QuitCommandPrinter createQuitCommandPrinter() {
        return QuitCommandPrinter.INSTANCE;
    }

    static HelpCommandPrinter createHelpCommandPrinter() {
        return HelpCommandPrinter.INSTANCE;
    }

    static StatementResultPrinter createStatementCommandPrinter(
            StatementResult result, ReadableConfig sessionConfig) {
        return new StatementResultPrinter(result, sessionConfig);
    }

    static InitializationCommandPrinter createInitializationCommandPrinter() {
        return InitializationCommandPrinter.INSTANCE;
    }

    // --------------------------------------------------------------------------------------------
    // Printers
    // --------------------------------------------------------------------------------------------

    /** Printer to print the HELP results. */
    class HelpCommandPrinter implements Printer {

        private static final HelpCommandPrinter INSTANCE = new HelpCommandPrinter();

        @Override
        public boolean isQuitCommand() {
            return false;
        }

        @Override
        public void print(Terminal terminal) {
            terminal.writer().println(CliStrings.MESSAGE_HELP);
            terminal.flush();
        }

        @Override
        public void close() {}
    }

    /** Printer to print the QUIT messages. */
    class QuitCommandPrinter implements Printer {

        private static final QuitCommandPrinter INSTANCE = new QuitCommandPrinter();

        @Override
        public boolean isQuitCommand() {
            return true;
        }

        @Override
        public void print(Terminal terminal) {
            printInfo(terminal, CliStrings.MESSAGE_QUIT);
        }

        @Override
        public void close() {}
    }

    /** Printer to clear the terminal. */
    class ClearCommandPrinter implements Printer {

        private static final ClearCommandPrinter INSTANCE = new ClearCommandPrinter();

        @Override
        public boolean isQuitCommand() {
            return false;
        }

        @Override
        public void print(Terminal terminal) {
            if (TerminalUtils.isPlainTerminal(terminal)) {
                for (int i = 0; i < 200; i++) { // large number of empty lines
                    terminal.writer().println();
                }
            } else {
                terminal.puts(InfoCmp.Capability.clear_screen);
            }
        }

        @Override
        public void close() {}
    }

    /** Printer prints the initialization command results. */
    class InitializationCommandPrinter implements Printer {

        static final InitializationCommandPrinter INSTANCE = new InitializationCommandPrinter();

        private InitializationCommandPrinter() {}

        @Override
        public boolean isQuitCommand() {
            return false;
        }

        @Override
        public void print(Terminal terminal) {
            printInfo(terminal, MESSAGE_EXECUTE_STATEMENT);
        }

        @Override
        public void close() {}
    }

    /** Printer prints the statement results. */
    class StatementResultPrinter implements Printer {

        private final StatementResult result;
        private final ReadableConfig sessionConfig;

        public StatementResultPrinter(StatementResult result, ReadableConfig sessionConfig) {
            this.result = result;
            this.sessionConfig = sessionConfig;
        }

        @Override
        public boolean isQuitCommand() {
            return false;
        }

        @Override
        public void print(Terminal terminal) {
            if (result.isQueryResult()) {
                printQuery(terminal);
            } else if (result.getJobId() != null) {
                printJob(terminal, result.getJobId());
            } else {
                defaultPrint(terminal);
            }
        }

        private void printQuery(Terminal terminal) {
            final ResultDescriptor resultDesc = new ResultDescriptor(result, sessionConfig);
            if (resultDesc.isTableauMode()) {
                try (CliTableauResultView tableauResultView =
                        new CliTableauResultView(terminal, resultDesc)) {
                    tableauResultView.displayResults();
                }
            } else {
                final CliResultView<?> view;
                if (resultDesc.isMaterialized()) {
                    view = new CliTableResultView(terminal, resultDesc);
                } else {
                    view = new CliChangelogResultView(terminal, resultDesc);
                }

                // enter view
                view.open();

                // view left
                printInfo(terminal, CliStrings.MESSAGE_RESULT_QUIT);
            }
        }

        private void printJob(Terminal terminal, JobID jobID) {
            if (sessionConfig.get(TABLE_DML_SYNC)) {
                printInfo(terminal, MESSAGE_FINISH_STATEMENT);
            } else {
                terminal.writer()
                        .println(CliStrings.messageInfo(MESSAGE_SUBMITTING_STATEMENT).toAnsi());
                terminal.writer()
                        .println(CliStrings.messageInfo(MESSAGE_STATEMENT_SUBMITTED).toAnsi());
                terminal.writer().println(String.format("Job ID: %s\n", jobID));
                terminal.flush();
            }
        }

        private void defaultPrint(Terminal terminal) {
            if (result.getResultKind() == ResultKind.SUCCESS) {
                // print more meaningful message than tableau OK result
                printInfo(terminal, MESSAGE_EXECUTE_STATEMENT);
            } else {
                // print tableau if result has content
                PrintStyle.tableauWithDataInferredColumnWidths(
                                result.getResultSchema(),
                                result.getRowDataToStringConverter(),
                                Integer.MAX_VALUE,
                                true,
                                false)
                        .print(result, terminal.writer());
            }
        }

        @Override
        public void close() {
            result.close();
        }
    }

    static void printInfo(Terminal terminal, String message) {
        terminal.writer().println(CliStrings.messageInfo(message).toAnsi());
        terminal.flush();
    }
}
