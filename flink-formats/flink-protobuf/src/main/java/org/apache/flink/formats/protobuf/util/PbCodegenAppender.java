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

package org.apache.flink.formats.protobuf.util;

import org.apache.flink.shaded.guava31.com.google.common.base.Strings;

/** Helper class which do code fragment concat. */
public class PbCodegenAppender {
    private static final int DEFAULT_INDENT = 2;
    private final StringBuilder sb;
    private int indent = 0;

    public PbCodegenAppender() {
        sb = new StringBuilder();
        this.indent = 0;
    }

    public PbCodegenAppender(int indent) {
        sb = new StringBuilder();
        this.indent = indent;
    }

    public void begin() {
        indent += DEFAULT_INDENT;
    }

    public void begin(String code) {
        sb.append(indent()).append(code).append("\n");
        begin();
    }

    public void end() {
        indent -= DEFAULT_INDENT;
    }

    public void end(String code) {
        end();
        sb.append(indent()).append(code).append("\n");
    }

    public int currentIndent() {
        return indent;
    }

    private String indent() {
        return Strings.repeat(" ", indent);
    }

    public void appendLine(String code) {
        sb.append(indent()).append(code).append(";\n");
    }

    public void appendSegment(String code) {
        sb.append(code).append("\n");
    }

    public String code() {
        return sb.toString();
    }

    public String printWithLineNumber() {
        StringBuilder newSb = new StringBuilder();
        String[] lines = sb.toString().split("\n");
        for (int i = 0; i < lines.length; i++) {
            newSb.append("Line ").append(i + 1).append(": ").append(lines[i]).append("\n");
        }
        return newSb.toString();
    }
}
