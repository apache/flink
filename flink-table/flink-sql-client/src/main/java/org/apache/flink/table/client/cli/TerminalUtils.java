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

import org.apache.flink.table.client.SqlClientException;

import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.terminal.impl.DumbTerminal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** Utilities for terminal handling. */
public class TerminalUtils {

    private TerminalUtils() {
        // do not instantiate
    }

    public static Terminal createDumbTerminal() {
        return createDumbTerminal(new MockOutputStream());
    }

    public static Terminal createDumbTerminal(OutputStream out) {
        try {
            return new DumbTerminal(new MockInputStream(), out);
        } catch (IOException e) {
            throw new SqlClientException("Unable to create dummy terminal.", e);
        }
    }

    public static Terminal createDumbTerminal(InputStream in, OutputStream out) {
        try {
            return new DumbTerminal(in, out);
        } catch (IOException e) {
            throw new SqlClientException("Unable to create dummy terminal.", e);
        }
    }

    public static Terminal createDefaultTerminal() {
        try {
            return TerminalBuilder.builder().name(CliStrings.CLI_NAME).build();
        } catch (IOException e) {
            throw new SqlClientException("Error opening command line interface.", e);
        }
    }

    private static class MockInputStream extends InputStream {

        @Override
        public int read() {
            return 0;
        }
    }

    /** A mock {@link OutputStream} for testing. */
    public static class MockOutputStream extends OutputStream {

        @Override
        public void write(int b) {
            // do nothing
        }
    }
}
