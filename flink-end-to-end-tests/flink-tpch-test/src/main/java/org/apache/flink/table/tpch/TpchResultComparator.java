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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/** Result comparator for TPC-H test, according to the TPC-H standard specification v2.18.0. */
public class TpchResultComparator {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println(
                    "Exactly 2 paths must be provided, the expected result path and the actual result path");
            System.exit(1);
        }

        String expectedPath = args[0];
        String actualPath = args[1];

        File[] partitions = new File(actualPath).listFiles();
        if (partitions == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "The specified actual result path: %s doesn't exists.", actualPath));
        }
        if (partitions.length > 1) {
            throw new UnsupportedOperationException(
                    "Please set the sink.parallelism 1 to keep the partition number is 1.");
        }

        try (BufferedReader expectedReader = new BufferedReader(new FileReader(expectedPath));
                BufferedReader actualReader = new BufferedReader(new FileReader(partitions[0]))) {
            int expectedLineNum = 0;
            int actualLineNum = 0;

            String expectedLine, actualLine;
            while ((expectedLine = expectedReader.readLine()) != null
                    && (actualLine = actualReader.readLine()) != null) {
                String[] expected = expectedLine.split("\\|");
                expectedLineNum++;
                String[] actual = actualLine.split("\\|");
                actualLineNum++;

                if (expected.length != actual.length) {
                    System.out.println(
                            "Incorrect number of columns on line "
                                    + actualLineNum
                                    + "! Expecting "
                                    + expected.length
                                    + " columns, but found "
                                    + actual.length
                                    + " columns.");
                    System.exit(1);
                }
                for (int i = 0; i < expected.length; i++) {
                    boolean failed;
                    try {
                        long e = Long.valueOf(expected[i]);
                        long a = Long.valueOf(actual[i]);
                        failed = (e != a);
                    } catch (NumberFormatException nfe) {
                        try {
                            double e = Double.valueOf(expected[i]);
                            double a = Double.valueOf(actual[i]);
                            if (e < 0 && a > 0 || e > 0 && a < 0) {
                                failed = true;
                            } else {
                                if (e < 0) {
                                    e = -e;
                                    a = -a;
                                }
                                double t = round(a, 2);
                                // defined in TPC-H standard specification v2.18.0 section 2.1.3.5
                                failed = (e * 0.99 > t || e * 1.01 < t);
                            }
                        } catch (NumberFormatException nfe2) {
                            failed =
                                    !expected[i]
                                            .trim()
                                            .equals(actual[i].replaceAll("\"", "").trim());
                        }
                    }
                    if (failed) {
                        System.out.println(
                                "Incorrect result on line "
                                        + actualLineNum
                                        + " column "
                                        + (i + 1)
                                        + "! Expecting "
                                        + expected[i]
                                        + ", but found "
                                        + actual[i]
                                        + ".");
                        System.exit(1);
                    }
                }
            }

            while (expectedReader.readLine() != null) {
                expectedLineNum++;
            }
            while (actualReader.readLine() != null) {
                actualLineNum++;
            }
            if (expectedLineNum != actualLineNum) {
                System.out.println(
                        "Incorrect number of lines! Expecting "
                                + expectedLineNum
                                + " lines, but found "
                                + actualLineNum
                                + " lines.");
                System.exit(1);
            }
        }
    }

    /** Rounding function defined in TPC-H standard specification v2.18.0 chapter 10. */
    private static double round(double x, int m) {
        if (x < 0) {
            throw new IllegalArgumentException("x must be non-negative");
        }
        double y = x + 5 * Math.pow(10, -m - 1);
        double z = y * Math.pow(10, m);
        double q = Math.floor(z);
        return q / Math.pow(10, m);
    }
}
