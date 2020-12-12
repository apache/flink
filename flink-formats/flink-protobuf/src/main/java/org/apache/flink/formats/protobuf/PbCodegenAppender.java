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

package org.apache.flink.formats.protobuf;

/** Helper class which do code fragment concat. */
public class PbCodegenAppender {
    private StringBuilder sb;

    public PbCodegenAppender() {
        sb = new StringBuilder();
    }

    public void appendLine(String code) {
        sb.append(code + ";\n");
    }

    public void appendSegment(String code) {
        sb.append(code + "\n");
    }

    public String code() {
        return sb.toString();
    }

    public static String printWithLineNumber(String code) {
        StringBuilder sb = new StringBuilder();
        String[] lines = code.split("\n");
        for (int i = 0; i < lines.length; i++) {
            sb.append("Line " + (i + 1) + ": " + lines[i] + "\n");
        }
        return sb.toString();
    }

    public String printWithLineNumber() {
        StringBuilder newSb = new StringBuilder();
        String[] lines = sb.toString().split("\n");
        for (int i = 0; i < lines.length; i++) {
            newSb.append("Line " + (i + 1) + ": " + lines[i] + "\n");
        }
        return newSb.toString();
    }
}
