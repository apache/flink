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
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExtendedOperationExecutor;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.operations.Operation;

import java.util.Optional;

/**
 * Factory that creates {@link Parser} and {@link ExtendedOperationExecutor}.
 *
 * <p>The {@link #factoryIdentifier()} is identified by matching it against {@link
 * TableConfigOptions#TABLE_SQL_DIALECT}.
 */
@Internal
public interface DialectFactory extends Factory {

    /** Creates a new parser. */
    Parser create(Context context);

    default ExtendedOperationExecutor createExtendedOperationExecutor(Context context) {
        return new EmptyOperationExecutor();
    }

    /** Context provided when a parser is created. */
    interface Context {
        CatalogManager getCatalogManager();

        PlannerContext getPlannerContext();

        Executor getExecutor();
    }

    /** Default implementation for {@link Context}. */
    class DefaultParserContext implements Context {
        private final CatalogManager catalogManager;
        private final PlannerContext plannerContext;
        private final Executor executor;

        public DefaultParserContext(
                CatalogManager catalogManager, PlannerContext plannerContext, Executor executor) {
            this.catalogManager = catalogManager;
            this.plannerContext = plannerContext;
            this.executor = executor;
        }

        @Override
        public CatalogManager getCatalogManager() {
            return catalogManager;
        }

        @Override
        public PlannerContext getPlannerContext() {
            return plannerContext;
        }

        @Override
        public Executor getExecutor() {
            return executor;
        }
    }

    /**
     * Default implementation for {@link ExtendedOperationExecutor} that doesn't extend any
     * operation behavior but forward all operations to the Flink planner.
     */
    class EmptyOperationExecutor implements ExtendedOperationExecutor {

        @Override
        public Optional<TableResultInternal> executeOperation(Operation operation) {
            // return empty so that it'll use Flink's own implementation for operation execution.
            return Optional.empty();
        }
    }
}
