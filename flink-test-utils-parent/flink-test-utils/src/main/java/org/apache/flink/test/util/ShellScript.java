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

package org.apache.flink.test.util;

import org.apache.flink.util.OperatingSystem;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

/** ShellScript for creating shell script on linux and windows. */
public class ShellScript {

    public static ShellScriptBuilder createShellScriptBuilder() {
        if (OperatingSystem.isWindows()) {
            return new WindowsShellScriptBuilder();
        }
        return new UnixShellScriptBuilder();
    }

    public static String getScriptExtension() {
        return OperatingSystem.isWindows() ? ".cmd" : ".sh";
    }

    /** Builder to create shell script. */
    public abstract static class ShellScriptBuilder {
        private static final String LINE_SEPARATOR = System.getProperty("line.separator");
        private final StringBuilder sb = new StringBuilder();

        void line(String... command) {
            for (String s : command) {
                sb.append(s);
            }
            sb.append(LINE_SEPARATOR);
        }

        public void write(File file) throws IOException {
            try (FileWriter fwrt = new FileWriter(file);
                    PrintWriter out = new PrintWriter(fwrt)) {
                out.append(sb);
            }
            file.setExecutable(true, false);
        }

        public abstract void command(List<String> command);

        public abstract void env(String key, String value);
    }

    private static final class UnixShellScriptBuilder extends ShellScriptBuilder {

        UnixShellScriptBuilder() {
            line("#!/usr/bin/env bash");
            line();
        }

        public void command(List<String> command) {
            line("exec ", String.join(" ", command));
        }

        public void env(String key, String value) {
            line("export ", key, "=\"", value, "\"");
        }
    }

    private static final class WindowsShellScriptBuilder extends ShellScriptBuilder {

        WindowsShellScriptBuilder() {
            line("@setlocal");
            line();
        }

        public void command(List<String> command) {
            line("@call ", String.join(" ", command));
        }

        public void env(String key, String value) {
            line("@set ", key, "=", value);
        }
    }
}
