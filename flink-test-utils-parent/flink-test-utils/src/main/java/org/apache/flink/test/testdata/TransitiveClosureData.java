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

package org.apache.flink.test.testdata;

import org.junit.jupiter.api.Assertions;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.regex.Pattern;

/** Test data for TransitiveClosure programs. */
public class TransitiveClosureData {

    public static void checkOddEvenResult(BufferedReader result) throws IOException {
        Pattern split = Pattern.compile(" ");
        String line;
        while ((line = result.readLine()) != null) {
            String[] res = split.split(line);
            Assertions.assertEquals(
                    2, res.length, "Malformed result: Wrong number of tokens in line.");
            try {
                int from = Integer.parseInt(res[0]);
                int to = Integer.parseInt(res[1]);

                Assertions.assertEquals(from % 2, to % 2, "Vertex should not be reachable.");
            } catch (NumberFormatException e) {
                Assertions.fail("Malformed result.");
            }
        }
    }

    private TransitiveClosureData() {}
}
