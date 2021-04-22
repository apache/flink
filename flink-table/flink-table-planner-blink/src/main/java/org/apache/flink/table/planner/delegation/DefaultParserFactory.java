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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** A Parser factory that creates {@link ParserImpl}. */
public class DefaultParserFactory implements ParserFactory {
    @Override
    public Parser create(CatalogManager catalogManager, PlannerContext plannerContext) {
        return new ParserImpl(
                catalogManager,
                () ->
                        plannerContext.createFlinkPlanner(
                                catalogManager.getCurrentCatalog(),
                                catalogManager.getCurrentDatabase()),
                plannerContext::createCalciteParser,
                plannerContext.getSqlExprToRexConverterFactory());
    }

    @Override
    public Map<String, String> optionalContext() {
        DescriptorProperties properties = new DescriptorProperties();
        return properties.asMap();
    }

    @Override
    public Map<String, String> requiredContext() {
        DescriptorProperties properties = new DescriptorProperties();
        properties.putString(
                TableConfigOptions.TABLE_SQL_DIALECT.key(),
                SqlDialect.DEFAULT.name().toLowerCase());
        return properties.asMap();
    }

    @Override
    public List<String> supportedProperties() {
        return Collections.singletonList(TableConfigOptions.TABLE_SQL_DIALECT.key());
    }
}
