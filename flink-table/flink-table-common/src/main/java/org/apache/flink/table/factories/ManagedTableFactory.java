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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.CatalogPartitionSpec;

import java.util.Map;

/**
 * Base interface for configuring a managed dynamic table connector. The managed table factory is
 * used when there is no {@link FactoryUtil#CONNECTOR} option.
 *
 * @deprecated This interface will be removed soon. Please see FLIP-346 for more details.
 */
@Deprecated
@Internal
public interface ManagedTableFactory extends DynamicTableFactory {

    /** {@link #factoryIdentifier()} for the managed table factory. */
    String DEFAULT_IDENTIFIER = "default";

    @Override
    default String factoryIdentifier() {
        return DEFAULT_IDENTIFIER;
    }

    /**
     * Enrich options from catalog and session information.
     *
     * @return new options of this table.
     */
    Map<String, String> enrichOptions(Context context);

    /** Notifies the listener that a table creation occurred. */
    void onCreateTable(Context context, boolean ignoreIfExists);

    /** Notifies the listener that a table drop occurred. */
    void onDropTable(Context context, boolean ignoreIfNotExists);

    /**
     * Notifies the listener that a table compaction occurred.
     *
     * @return dynamic options of the source and sink info for this table.
     */
    Map<String, String> onCompactTable(Context context, CatalogPartitionSpec partitionSpec);

    /** Discovers the unique implementation of {@link ManagedTableFactory} without identifier. */
    static ManagedTableFactory discoverManagedTableFactory(ClassLoader classLoader) {
        return FactoryUtil.discoverManagedTableFactory(classLoader, ManagedTableFactory.class);
    }
}
