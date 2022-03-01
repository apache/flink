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

package org.apache.flink.connector.base.sink;

import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

/** Dummy destination where the imaginary concrete sink is writing to. */
public class ArrayListDestination {

    private static BlockingDeque<Integer> store = new LinkedBlockingDeque<>();

    /**
     * Returns a list of indices of elements that failed to insert, fails to insert if the integer
     * value of the {@code newRecord} is greater than 1000.
     */
    protected static List<Integer> putRecords(List<Integer> newRecords) {
        store.addAll(
                newRecords.stream().filter(record -> record <= 1000).collect(Collectors.toList()));
        if (newRecords.contains(1_000_000)) {
            throw new RuntimeException(
                    "Intentional error on persisting 1_000_000 to ArrayListDestination");
        }
        return newRecords.stream().filter(record -> record > 1000).collect(Collectors.toList());
    }
}
