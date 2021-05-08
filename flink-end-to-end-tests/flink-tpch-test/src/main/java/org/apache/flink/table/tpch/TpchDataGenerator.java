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

package org.apache.flink.table.tpch;

import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

/** TPC-H test data generator. */
public class TpchDataGenerator {

    public static final int QUERY_NUM = 22;

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("Exactly 1 double value and 1 path should be provided as argument");
            return;
        }

        double scale = Double.valueOf(args[0]);
        String path = args[1];
        generateTable(scale, path);
        generateQuery(path);
        generateExpected(path);
    }

    private static void generateTable(double scale, String path) throws IOException {
        File dir = new File(path + "/table");
        dir.mkdir();

        for (TpchTable table : TpchTable.getTables()) {
            Iterable generator = table.createGenerator(scale, 1, 1);

            StringBuilder builder = new StringBuilder();
            generator.forEach(
                    s -> {
                        String line = ((TpchEntity) s).toLine().trim();
                        if (line.endsWith("|")) {
                            line = line.substring(0, line.length() - 1);
                        }
                        builder.append(line).append('\n');
                    });

            try (BufferedWriter writer =
                    new BufferedWriter(
                            new OutputStreamWriter(
                                    new FileOutputStream(
                                            path + "/table/" + table.getTableName() + ".csv")))) {
                writer.write(builder.toString());
            }
        }
    }

    private static void generateQuery(String path) throws IOException {
        File dir = new File(path + "/query");
        dir.mkdir();

        for (int i = 0; i < QUERY_NUM; i++) {
            try (InputStream in =
                            TpchDataGenerator.class.getResourceAsStream(
                                    "/io/airlift/tpch/queries/q" + (i + 1) + ".sql");
                    OutputStream out = new FileOutputStream(path + "/query/q" + (i + 1) + ".sql")) {
                byte[] buffer = new byte[4096];
                int bytesRead = 0;
                while ((bytesRead = in.read(buffer)) > 0) {
                    out.write(buffer, 0, bytesRead);
                }
            }
        }
    }

    private static void generateExpected(String path) throws IOException {
        File dir = new File(path + "/expected");
        dir.mkdir();

        for (int i = 0; i < QUERY_NUM; i++) {
            try (BufferedReader reader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            TpchDataGenerator.class.getResourceAsStream(
                                                    "/io/airlift/tpch/queries/q"
                                                            + (i + 1)
                                                            + ".result")));
                    BufferedWriter writer =
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            new FileOutputStream(
                                                    path + "/expected/q" + (i + 1) + ".csv")))) {
                int lineNumber = 0;
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim().replace("null", "");
                    lineNumber++;
                    if (lineNumber == 1) {
                        continue;
                    }
                    if (line.length() > 0 && line.endsWith("|")) {
                        line = line.substring(0, line.length() - 1);
                    }
                    writer.write(line + "\n");
                }
            }
        }
    }
}
