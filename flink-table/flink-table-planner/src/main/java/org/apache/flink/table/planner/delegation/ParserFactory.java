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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.factories.Factory;

/**
 * Factory that creates {@link Parser}.
 *
 * <p>The {@link #factoryIdentifier()} is identified by matching it against {@link
 * TableConfigOptions#TABLE_SQL_DIALECT}.
 */
@Internal
public interface ParserFactory extends Factory {

    /** Creates a new parser. */
    Parser create(Context context);

    /** Context provided when a parser is created. */
    interface Context {
        CatalogManager getCatalogManager();

        PlannerContext getPlannerContext();
    }

    /** Default implementation for {@link Context}. */
    class DefaultParserContext implements Context {
        private final CatalogManager catalogManager;
        private final PlannerContext plannerContext;

        public DefaultParserContext(CatalogManager catalogManager, PlannerContext plannerContext) {
            this.catalogManager = catalogManager;
            this.plannerContext = plannerContext;
        }

        @Override
        public CatalogManager getCatalogManager() {
            return catalogManager;
        }

        @Override
        public PlannerContext getPlannerContext() {
            return plannerContext;
        }
    }
}
