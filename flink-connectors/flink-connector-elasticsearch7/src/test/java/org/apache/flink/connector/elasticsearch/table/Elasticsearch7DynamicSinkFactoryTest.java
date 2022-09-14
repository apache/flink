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

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.elasticsearch.table.TestContext.context;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for validation in {@link Elasticsearch7DynamicSinkFactory}. */
public class Elasticsearch7DynamicSinkFactoryTest extends ElasticsearchDynamicSinkFactoryBaseTest {
    @Override
    ElasticsearchDynamicSinkFactoryBase createSinkFactory() {
        return new Elasticsearch7DynamicSinkFactory();
    }

    @Override
    TestContext createPrefilledTestContext() {
        return context()
                .withOption(ElasticsearchConnectorOptions.INDEX_OPTION.key(), "MyIndex")
                .withOption(
                        ElasticsearchConnectorOptions.HOSTS_OPTION.key(), "http://localhost:12345");
    }

    @Test
    public void validateEmptyConfiguration() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();

        assertThatThrownBy(() -> sinkFactory.createDynamicTableSink(context().build()))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "One or more required options are missing.\n"
                                + "\n"
                                + "Missing required options are:\n"
                                + "\n"
                                + "hosts\n"
                                + "index");
    }
}
