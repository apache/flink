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

package org.apache.flink.connector.pulsar.testutils;

import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;
import org.apache.flink.connectors.test.common.external.ExternalContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

/** Common test context for pulsar based test. */
public abstract class PulsarTestContext<T> implements ExternalContext<T> {
    private static final long serialVersionUID = 8109719617929996743L;

    protected final PulsarRuntimeOperator operator;

    protected PulsarTestContext(PulsarTestEnvironment environment) {
        this.operator = environment.operator();
    }

    // Helper methods for generating data.

    protected List<String> generateStringTestData(int splitIndex, long seed) {
        Random random = new Random(seed);
        int recordNum = 300 + random.nextInt(200);
        List<String> records = new ArrayList<>(recordNum);

        for (int i = 0; i < recordNum; i++) {
            int length = random.nextInt(40) + 10;
            records.add(splitIndex + "-" + i + "-" + randomAlphanumeric(length));
        }

        return records;
    }

    protected abstract String displayName();

    @Override
    public String toString() {
        return displayName();
    }
}
