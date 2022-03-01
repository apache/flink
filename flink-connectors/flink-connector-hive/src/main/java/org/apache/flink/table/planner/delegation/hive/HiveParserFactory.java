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

package org.apache.flink.table.planner.delegation.hive;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.planner.delegation.ParserFactory;

import java.util.Collections;
import java.util.Set;

/** A Parser factory that creates {@link HiveParser}. */
public class HiveParserFactory implements ParserFactory {

    @Override
    public String factoryIdentifier() {
        return SqlDialect.HIVE.name().toLowerCase();
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public Parser create(Context context) {
        return new HiveParser(
                context.getCatalogManager(),
                () ->
                        context.getPlannerContext()
                                .createFlinkPlanner(
                                        context.getCatalogManager().getCurrentCatalog(),
                                        context.getCatalogManager().getCurrentDatabase()),
                context.getPlannerContext()::createCalciteParser,
                context.getPlannerContext());
    }
}
