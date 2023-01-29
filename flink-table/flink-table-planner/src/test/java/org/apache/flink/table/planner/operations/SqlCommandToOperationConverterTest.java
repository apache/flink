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

package org.apache.flink.table.planner.operations;

import org.apache.flink.table.operations.BeginStatementSetOperation;
import org.apache.flink.table.operations.EndStatementSetOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.command.AddJarOperation;
import org.apache.flink.table.operations.command.ClearOperation;
import org.apache.flink.table.operations.command.HelpOperation;
import org.apache.flink.table.operations.command.QuitOperation;
import org.apache.flink.table.operations.command.RemoveJarOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.table.planner.parse.ExtendedParser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test cases for the commands for {@link SqlToOperationConverter}. */
public class SqlCommandToOperationConverterTest extends SqlToOperationConverterTestBase {

    @Test
    public void testBeginStatementSet() {
        final String sql = "BEGIN STATEMENT SET";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(BeginStatementSetOperation.class);
        final BeginStatementSetOperation beginStatementSetOperation =
                (BeginStatementSetOperation) operation;

        assertThat(beginStatementSetOperation.asSummaryString()).isEqualTo("BEGIN STATEMENT SET");
    }

    @Test
    public void testEnd() {
        final String sql = "END";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(EndStatementSetOperation.class);
        final EndStatementSetOperation endStatementSetOperation =
                (EndStatementSetOperation) operation;

        assertThat(endStatementSetOperation.asSummaryString()).isEqualTo("END");
    }

    @Test
    public void testAddJar() {
        Arrays.asList(
                        "./test.\njar",
                        "file:///path/to/whatever",
                        "../test-jar.jar",
                        "/root/test.jar",
                        "test\\ jar.jar",
                        "oss://path/helloworld.go")
                .forEach(
                        jarPath -> {
                            AddJarOperation operation =
                                    (AddJarOperation)
                                            parser.parse(String.format("ADD JAR '%s'", jarPath))
                                                    .get(0);
                            assertThat(operation.getPath()).isEqualTo(jarPath);
                        });
    }

    @Test
    public void testRemoveJar() {
        Arrays.asList(
                        "./test.\njar",
                        "file:///path/to/whatever",
                        "../test-jar.jar",
                        "/root/test.jar",
                        "test\\ jar.jar",
                        "oss://path/helloworld.go")
                .forEach(
                        jarPath -> {
                            RemoveJarOperation operation =
                                    (RemoveJarOperation)
                                            parser.parse(String.format("REMOVE JAR '%s'", jarPath))
                                                    .get(0);
                            assertThat(operation.getPath()).isEqualTo(jarPath);
                        });
    }

    @Test
    public void testSet() {
        Operation operation1 = parse("SET");
        assertThat(operation1).isInstanceOf(SetOperation.class);
        SetOperation setOperation1 = (SetOperation) operation1;
        assertThat(setOperation1.getKey()).isNotPresent();
        assertThat(setOperation1.getValue()).isNotPresent();

        Operation operation2 = parse("SET 'test-key' = 'test-value'");
        assertThat(operation2).isInstanceOf(SetOperation.class);
        SetOperation setOperation2 = (SetOperation) operation2;
        assertThat(setOperation2.getKey()).hasValue("test-key");
        assertThat(setOperation2.getValue()).hasValue("test-value");
    }

    @Test
    public void testReset() {
        Operation operation1 = parse("RESET");
        assertThat(operation1).isInstanceOf(ResetOperation.class);
        assertThat(((ResetOperation) operation1).getKey()).isNotPresent();

        Operation operation2 = parse("RESET 'test-key'");
        assertThat(operation2).isInstanceOf(ResetOperation.class);
        assertThat(((ResetOperation) operation2).getKey()).isPresent();
        assertThat(((ResetOperation) operation2).getKey()).hasValue("test-key");
    }

    @ParameterizedTest
    @ValueSource(strings = {"SET", "SET;", "SET ;", "SET\t;", "SET\n;"})
    public void testSetCommands(String command) {
        ExtendedParser extendedParser = new ExtendedParser();
        assertThat(extendedParser.parse(command)).get().isInstanceOf(SetOperation.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"HELP", "HELP;", "HELP ;", "HELP\t;", "HELP\n;"})
    public void testHelpCommands(String command) {
        ExtendedParser extendedParser = new ExtendedParser();
        assertThat(extendedParser.parse(command)).get().isInstanceOf(HelpOperation.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"CLEAR", "CLEAR;", "CLEAR ;", "CLEAR\t;", "CLEAR\n;"})
    public void testClearCommands(String command) {
        ExtendedParser extendedParser = new ExtendedParser();
        assertThat(extendedParser.parse(command)).get().isInstanceOf(ClearOperation.class);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "QUIT;", "QUIT;", "QUIT ;", "QUIT\t;", "QUIT\n;", "EXIT;", "EXIT ;", "EXIT\t;",
                "EXIT\n;", "EXIT ; "
            })
    public void testQuitCommands(String command) {
        ExtendedParser extendedParser = new ExtendedParser();
        assertThat(extendedParser.parse(command)).get().isInstanceOf(QuitOperation.class);
    }
}
