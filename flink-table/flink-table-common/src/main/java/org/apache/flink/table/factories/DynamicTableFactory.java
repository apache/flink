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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Base interface for configuring a dynamic table connector for an external storage system from
 * catalog and session information.
 *
 * <p>Dynamic tables are the core concept of Flink's Table & SQL API for processing both bounded and
 * unbounded data in a unified fashion.
 *
 * <p>Implement {@link DynamicTableSourceFactory} for constructing a {@link DynamicTableSource}.
 *
 * <p>Implement {@link DynamicTableSinkFactory} for constructing a {@link DynamicTableSink}.
 *
 * <p>The options {@link FactoryUtil#PROPERTY_VERSION} and {@link FactoryUtil#CONNECTOR} are
 * implicitly added and must not be declared.
 */
@PublicEvolving
public interface DynamicTableFactory extends Factory {

    /**
     * Returns a set of {@link ConfigOption} that are directly forwarded to the runtime
     * implementation but don't affect the final execution topology.
     *
     * <p>Options declared here can override options of the persisted plan during an enrichment
     * phase. Since a restored topology is static, an implementer has to ensure that the declared
     * options don't affect fundamental abilities such as {@link SupportsProjectionPushDown} or
     * {@link SupportsFilterPushDown}.
     *
     * <p>For example, given a database connector, if an option defines the connection timeout,
     * changing this value does not affect the pipeline topology and can be allowed. However, an
     * option that defines whether the connector supports {@link SupportsReadingMetadata} or not is
     * not allowed. The planner might not react to changed abilities anymore.
     *
     * @see DynamicTableFactory.Context#getEnrichmentOptions()
     * @see TableFactoryHelper#getOptions()
     * @see FormatFactory#forwardOptions()
     */
    default Set<ConfigOption<?>> forwardOptions() {
        return Collections.emptySet();
    }

    /** Provides catalog and session information describing the dynamic table to be accessed. */
    @PublicEvolving
    interface Context {

        /**
         * Returns the identifier of the table in the {@link Catalog}.
         *
         * <p>This identifier describes the relationship between the table instance and the
         * associated {@link Catalog} (if any). However, it doesn't uniquely identify this specific
         * table configuration. The same table might be stored in different catalogs or, in case of
         * anonymous tables, this identifier is auto-generated and non-deterministic. Because of
         * that behaviour, we strongly suggest using this identifier only for printing and logging
         * purposes, and rely on user input for uniquely identifying a "table instance".
         *
         * <p>For example, when implementing a Kafka source using consumer groups, the user should
         * provide the consumer group id manually rather than using this identifier as the consumer
         * group id, so the offset tracking remains stable even if this table is anonymous, or it's
         * moved to another {@link Catalog}.
         *
         * <p>Note that for anonymous tables {@link ObjectIdentifier#asSerializableString()} will
         * fail, so we suggest to use {@link ObjectIdentifier#asSummaryString()} for generating
         * strings.
         */
        ObjectIdentifier getObjectIdentifier();

        /**
         * Returns the resolved table information received from the {@link Catalog} or persisted
         * plan.
         *
         * <p>The {@link ResolvedCatalogTable} forwards the metadata from the catalog but offers a
         * validated {@link ResolvedSchema}. The original metadata object is available via {@link
         * ResolvedCatalogTable#getOrigin()}.
         *
         * <p>In most cases, a factory is interested in the following characteristics:
         *
         * <pre>{@code
         * // get the physical data type to initialize the connector
         * context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType()
         *
         * // get primary key information if the connector supports upserts
         * context.getCatalogTable().getResolvedSchema().getPrimaryKey()
         *
         * // get configuration options
         * context.getCatalogTable().getOptions()
         * }</pre>
         *
         * <p>Other characteristics such as metadata columns or watermarks will be pushed down into
         * the created {@link DynamicTableSource} or {@link DynamicTableSink} during planning
         * depending on the implemented ability interfaces.
         *
         * <p>During a plan restore, usually the table information persisted in the plan is used to
         * reconstruct the catalog table. If and only if {@code table.plan.restore.catalog-objects}
         * is set to {@code ALL}, there might be information from both the plan and a catalog lookup
         * which requires considering {@link #getEnrichmentOptions()}. It enables to enrich the plan
         * information with frequently changing options (e.g. connection information or timeouts).
         */
        ResolvedCatalogTable getCatalogTable();

        /**
         * Returns a map of options that can enrich the options of the original {@link
         * #getCatalogTable()} during a plan restore.
         *
         * <p>If and only if {@code table.plan.restore.catalog-objects} is set to {@code ALL}, this
         * method may return a non-empty {@link Map} of options retrieved from the {@link Catalog}.
         *
         * <p>Because only the {@link DynamicTableFactory} is able to decide which options are safe
         * to be forwarded without affecting the original topology, enrichment options are exposed
         * through this method. In general, it's highly recommended using the {@link
         * FactoryUtil#createTableFactoryHelper(DynamicTableFactory, Context)} to merge the options
         * and then get the result with {@link TableFactoryHelper#getOptions()}. The helper
         * considers both {@link #forwardOptions()} and {@link FormatFactory#forwardOptions()}.
         *
         * <p>Since a restored topology is static, an implementer has to ensure that the declared
         * options don't affect fundamental abilities. The planner might not react to changed
         * abilities anymore.
         *
         * @see TableFactoryHelper
         */
        default Map<String, String> getEnrichmentOptions() {
            return Collections.emptyMap();
        }

        /** Gives read-only access to the configuration of the current session. */
        ReadableConfig getConfiguration();

        /**
         * Returns the class loader of the current session.
         *
         * <p>The class loader is in particular useful for discovering further (nested) factories.
         */
        ClassLoader getClassLoader();

        /** Whether the table is temporary. */
        boolean isTemporary();

        /**
         * Returns the physical schema to use for encoding and decoding records. The returned row
         * data type contains only physical columns. It does not include computed or metadata
         * columns. A factory can use the returned data type to configure the table connector, and
         * can manipulate it using the {@link DataType} static methods:
         *
         * <pre>{@code
         * // Project some fields into a new data type
         * DataType projectedDataType = Projection.of(projectedIndexes)
         *     .project(context.getPhysicalRowDataType());
         *
         * // Create key data type
         * DataType keyDataType = Projection.of(context.getPrimaryKeyIndexes())
         *     .project(context.getPhysicalRowDataType());
         *
         * // Create a new data type filtering columns of the original data type
         * DataType myOwnDataType = DataTypes.ROW(
         *      DataType.getFields(context.getPhysicalRowDataType())
         *          .stream()
         *          .filter(myFieldFilterPredicate)
         *          .toArray(DataTypes.Field[]::new))
         * }</pre>
         *
         * <p>Shortcut for {@code getCatalogTable().getResolvedSchema().toPhysicalRowDataType()}.
         *
         * @see ResolvedSchema#toPhysicalRowDataType()
         */
        default DataType getPhysicalRowDataType() {
            return getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        }

        /**
         * Returns the primary key indexes, if any, otherwise returns an empty array. A factory can
         * use it to compute the schema projection of the key fields with {@code
         * Projection.of(ctx.getPrimaryKeyIndexes()).project(dataType)}.
         *
         * <p>Shortcut for {@code getCatalogTable().getResolvedSchema().getPrimaryKeyIndexes()}.
         *
         * @see ResolvedSchema#getPrimaryKeyIndexes()
         */
        default int[] getPrimaryKeyIndexes() {
            return getCatalogTable().getResolvedSchema().getPrimaryKeyIndexes();
        }
    }
}
