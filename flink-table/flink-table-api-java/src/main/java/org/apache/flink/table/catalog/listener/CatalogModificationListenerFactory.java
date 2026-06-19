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

package org.apache.flink.table.catalog.listener;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.Factory;

import java.util.Collections;
import java.util.Set;

/**
 * A factory to create catalog modification listener instances based on context which contains job
 * configuration and user classloader.
 */
@PublicEvolving
public interface CatalogModificationListenerFactory extends Factory {
    /** Creates and configures a {@link CatalogModificationListener} using the given context. */
    CatalogModificationListener createListener(Context context);

    @Override
    default Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    default Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    /** Context provided when a listener is created. */
    @PublicEvolving
    interface Context {
        /** Returns the read-only job configuration with which the listener is created. */
        ReadableConfig getConfiguration();

        /** Returns the class loader of the current job. */
        ClassLoader getUserClassLoader();
    }
}
