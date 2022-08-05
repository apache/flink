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

package org.apache.flink.streaming.connectors.dynamodb.sink;

import org.assertj.core.api.Assertions;
import org.junit.Test;

/** Tests for {@link DynamoDbSinkBuilder}. */
public class DynamoDbSinkBuilderTest {

    @Test
    public void testCreateDynamoDbSinkBuilder() {
        DynamoDbSink<String> dynamoDbSink =
                DynamoDbSink.<String>builder()
                        .setDynamoDbRequestConverter(
                                new DynamoDbRequestConverter<String>() {
                                    @Override
                                    public DynamoDbRequest apply(String element) {
                                        return null;
                                    }
                                })
                        .build();

        Assertions.assertThat(dynamoDbSink.getWriterStateSerializer().getVersion()).isEqualTo(1);
    }

    @Test
    public void dynamoDbRequestConverterOfSinkMustBeSetWhenBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> DynamoDbSink.builder().setFailOnError(true).build())
                .withMessageContaining(
                        "No DynamoDbRequestConverter was supplied to the DynamoDbSink builder.");
    }
}
