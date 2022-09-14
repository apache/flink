/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.io;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link PrintingOutputFormat}. */
class PrintingOutputFormatTest {

    private final PrintStream originalSystemOut = System.out;
    private final PrintStream originalSystemErr = System.err;

    private final ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
    private final ByteArrayOutputStream arrayErrorStream = new ByteArrayOutputStream();

    private final String line = System.lineSeparator();

    @BeforeEach
    void setUp() {
        System.setOut(new PrintStream(arrayOutputStream));
        System.setErr(new PrintStream(arrayErrorStream));
    }

    @AfterEach
    void tearDown() {
        if (System.out != originalSystemOut) {
            System.out.close();
        }
        if (System.err != originalSystemErr) {
            System.err.close();
        }
        System.setOut(originalSystemOut);
        System.setErr(originalSystemErr);
    }

    @Test
    void testPrintOutputFormatStdOut() {
        PrintingOutputFormat<String> printSink = new PrintingOutputFormat<>();
        printSink.open(0, 1);

        printSink.writeRecord("hello world!");

        assertThat(printSink).hasToString("Print to System.out");
        assertThat(arrayOutputStream).hasToString("hello world!" + line);
        printSink.close();
    }

    @Test
    void testPrintOutputFormatStdErr() {
        PrintingOutputFormat<String> printSink = new PrintingOutputFormat<>(true);
        printSink.open(0, 1);

        printSink.writeRecord("hello world!");

        assertThat(printSink).hasToString("Print to System.err");
        assertThat(arrayErrorStream).hasToString("hello world!" + line);
        printSink.close();
    }

    @Test
    void testPrintOutputFormatWithPrefix() {
        PrintingOutputFormat<String> printSink = new PrintingOutputFormat<>();
        printSink.open(1, 2);

        printSink.writeRecord("hello world!");

        assertThat(printSink).hasToString("Print to System.out");
        assertThat(arrayOutputStream).hasToString("2> hello world!" + line);
        printSink.close();
    }

    @Test
    void testPrintOutputFormatWithIdentifierAndPrefix() {
        PrintingOutputFormat<String> printSink = new PrintingOutputFormat<>("mySink", false);
        printSink.open(1, 2);

        printSink.writeRecord("hello world!");

        assertThat(printSink).hasToString("Print to System.out");
        assertThat(arrayOutputStream).hasToString("mySink:2> hello world!" + line);
        printSink.close();
    }

    @Test
    void testPrintOutputFormatWithIdentifierButNoPrefix() {
        PrintingOutputFormat<String> printSink = new PrintingOutputFormat<>("mySink", false);
        printSink.open(0, 1);

        printSink.writeRecord("hello world!");

        assertThat(printSink).hasToString("Print to System.out");
        assertThat(arrayOutputStream).hasToString("mySink> hello world!" + line);
        printSink.close();
    }
}
