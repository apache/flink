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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch6SinkBuilder;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.Set;

import static org.apache.flink.connector.elasticsearch.table.Elasticsearch6ConnectorOptions.DOCUMENT_TYPE_OPTION;

/** A {@link DynamicTableSinkFactory} for discovering {@link ElasticsearchDynamicSink}. */
@Internal
public class Elasticsearch6DynamicSinkFactory extends ElasticsearchDynamicSinkFactoryBase {
    private static final String FACTORY_IDENTIFIER = "elasticsearch-6";

    public Elasticsearch6DynamicSinkFactory() {
        super(FACTORY_IDENTIFIER, Elasticsearch6SinkBuilder::new);
    }

    @Override
    ElasticsearchConfiguration getConfiguration(Context context) {
        return new Elasticsearch6Configuration(
                Configuration.fromMap(context.getCatalogTable().getOptions()));
    }

    @Nullable
    @Override
    String getDocumentType(Context context) {
        Elasticsearch6Configuration config =
                (Elasticsearch6Configuration) getConfiguration(context);
        return config.getDocumentType();
    }

    @Override
    void validateConfiguration(ElasticsearchConfiguration config) {
        super.validateConfiguration(config);
        Elasticsearch6Configuration configuration = (Elasticsearch6Configuration) config;
        validate(
                !StringUtils.isNullOrWhitespaceOnly(configuration.getDocumentType()),
                () -> String.format("'%s' must not be empty", DOCUMENT_TYPE_OPTION.key()));
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = super.requiredOptions();
        requiredOptions.add(DOCUMENT_TYPE_OPTION);
        return requiredOptions;
    }
}
