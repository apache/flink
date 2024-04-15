/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api.config;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.catalog.Catalog;

import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * This class holds {@link org.apache.flink.configuration.ConfigOption}s used by table planner.
 *
 * <p>NOTE: All option keys in this class must start with "table".
 */
@PublicEvolving
public class TableConfigOptions {
    private TableConfigOptions() {}

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<String> TABLE_CATALOG_NAME =
            key("table.builtin-catalog-name")
                    .stringType()
                    .defaultValue("default_catalog")
                    .withDescription(
                            "The name of the initial catalog to be created when "
                                    + "instantiating a TableEnvironment.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<String> TABLE_DATABASE_NAME =
            key("table.builtin-database-name")
                    .stringType()
                    .defaultValue("default_database")
                    .withDescription(
                            "The name of the default database in the initial catalog to be created "
                                    + "when instantiating TableEnvironment.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<List<String>> TABLE_CATALOG_MODIFICATION_LISTENERS =
            key("table.catalog-modification.listeners")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "A (semicolon-separated) list of factories that creates listener for catalog "
                                    + "modification which will be notified in catalog manager after it "
                                    + "performs database and table ddl operations successfully.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_DML_SYNC =
            key("table.dml-sync")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Specifies if the DML job (i.e. the insert operation) is executed asynchronously or synchronously. "
                                    + "By default, the execution is async, so you can submit multiple DML jobs at the same time. "
                                    + "If set this option to true, the insert operation will wait for the job to finish.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED =
            key("table.dynamic-table-options.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Enable or disable the OPTIONS hint used to specify table options "
                                    + "dynamically, if disabled, an exception would be thrown "
                                    + "if any OPTIONS hint is specified");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<String> TABLE_SQL_DIALECT =
            key("table.sql-dialect")
                    .stringType()
                    .defaultValue(SqlDialect.DEFAULT.name().toLowerCase())
                    .withDescription(
                            "The SQL dialect defines how to parse a SQL query. "
                                    + "A different SQL dialect may support different SQL grammar. "
                                    + "Currently supported dialects are: default and hive");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<String> LOCAL_TIME_ZONE =
            key("table.local-time-zone")
                    .stringType()
                    // special value to decide whether to use ZoneId.systemDefault() in
                    // TableConfig.getLocalTimeZone()
                    .defaultValue("default")
                    .withDescription(
                            "The local time zone defines current session time zone id. It is used when converting to/from "
                                    + "<code>TIMESTAMP WITH LOCAL TIME ZONE</code>. Internally, timestamps with local time zone are always represented in the UTC time zone. "
                                    + "However, when converting to data types that don't include a time zone (e.g. TIMESTAMP, TIME, or simply STRING), "
                                    + "the session time zone is used during conversion. The input of option is either a full name "
                                    + "such as \"America/Los_Angeles\", or a custom timezone id such as \"GMT-08:00\".");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Integer> DISPLAY_MAX_COLUMN_WIDTH =
            key("table.display.max-column-width")
                    .intType()
                    .defaultValue(30)
                    .withDescription(
                            "When printing the query results to the client console, this parameter determines the number of characters shown on screen before truncating. "
                                    + "This only applies to columns with variable-length types (e.g. CHAR, VARCHAR, STRING) in the streaming mode. "
                                    + "Fixed-length types are printed in the batch mode using a deterministic column width.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    @Documentation.OverrideDefault("System.getProperty(\"java.io.tmpdir\")")
    public static final ConfigOption<String> RESOURCES_DOWNLOAD_DIR =
            key("table.resources.download-dir")
                    .stringType()
                    .defaultValue(System.getProperty("java.io.tmpdir"))
                    .withDescription(
                            "Local directory that is used by planner for storing downloaded resources.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_RTAS_CTAS_ATOMICITY_ENABLED =
            key("table.rtas-ctas.atomicity-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Specifies if the CREATE TABLE/REPLACE TABLE/CREATE OR REPLACE AS SELECT statement is executed atomically. By default, the statement is non-atomic. "
                                    + "The target table is created/replaced on the client side, and it will not be rolled back even though the job fails or is canceled. "
                                    + "If set this option to true and the underlying DynamicTableSink implements the SupportsStaging interface, "
                                    + "the statement is expected to be executed atomically, the behavior of which depends on the actual DynamicTableSink.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<List<ColumnExpansionStrategy>>
            TABLE_COLUMN_EXPANSION_STRATEGY =
                    key("table.column-expansion-strategy")
                            .enumType(ColumnExpansionStrategy.class)
                            .asList()
                            .defaultValues()
                            .withDescription(
                                    "Configures the default expansion behavior of 'SELECT *'. "
                                            + "By default, all top-level columns of the table's "
                                            + "schema are selected and nested fields are retained.");

    // ------------------------------------------------------------------------------------------
    // Options for plan handling
    // ------------------------------------------------------------------------------------------

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<CatalogPlanCompilation> PLAN_COMPILE_CATALOG_OBJECTS =
            key("table.plan.compile.catalog-objects")
                    .enumType(CatalogPlanCompilation.class)
                    .defaultValue(CatalogPlanCompilation.ALL)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Strategy how to persist catalog objects such as tables, "
                                                    + "functions, or data types into a plan during compilation.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "It influences the need for catalog metadata to be present "
                                                    + "during a restore operation and affects the plan size.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "This configuration option does not affect anonymous/inline "
                                                    + "or temporary objects. Anonymous/inline objects will "
                                                    + "be persisted entirely (including schema and options) "
                                                    + "if possible or fail the compilation otherwise. "
                                                    + "Temporary objects will be persisted only by their "
                                                    + "identifier and the object needs to be present in "
                                                    + "the session context during a restore.")
                                    .build());

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<CatalogPlanRestore> PLAN_RESTORE_CATALOG_OBJECTS =
            key("table.plan.restore.catalog-objects")
                    .enumType(CatalogPlanRestore.class)
                    .defaultValue(CatalogPlanRestore.ALL)
                    .withDescription(
                            "Strategy how to restore catalog objects such as tables, functions, or data "
                                    + "types using a given plan and performing catalog lookups if "
                                    + "necessary. It influences the need for catalog metadata to be"
                                    + "present and enables partial enrichment of plan information.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Boolean> PLAN_FORCE_RECOMPILE =
            key("table.plan.force-recompile")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When false COMPILE PLAN statement will fail if the output plan file is already existing, "
                                    + "unless the clause IF NOT EXISTS is used. "
                                    + "When true COMPILE PLAN will overwrite the existing output plan file. "
                                    + "We strongly suggest to enable this flag only for debugging purpose.");

    // ------------------------------------------------------------------------------------------
    // Options for code generation
    // ------------------------------------------------------------------------------------------

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Integer> MAX_LENGTH_GENERATED_CODE =
            key("table.generated-code.max-length")
                    .intType()
                    .defaultValue(4000)
                    .withDescription(
                            "Specifies a threshold where generated code will be split into sub-function calls. "
                                    + "Java has a maximum method length of 64 KB. This setting allows for finer granularity if necessary. "
                                    + "Default value is 4000 instead of 64KB as by default JIT refuses to work on methods with more than 8K byte code.");

    @Documentation.ExcludeFromDocumentation(
            "This option is rarely used. The default value is good enough for almost all cases.")
    public static final ConfigOption<Integer> MAX_MEMBERS_GENERATED_CODE =
            key("table.generated-code.max-members")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "Specifies a threshold where class members of generated code will be grouped into arrays by types.");

    // ------------------------------------------------------------------------------------------
    // Enum option types
    // ------------------------------------------------------------------------------------------

    /**
     * Strategy to compile {@link Catalog} objects into a plan.
     *
     * <p>Depending on the configuration, permanent catalog metadata (such as information about
     * tables and functions) will be persisted in the plan as well. Anonymous/inline objects will be
     * persisted (including schema and options) if possible or fail the compilation otherwise. For
     * temporary objects, only the identifier is part of the plan and the object needs to be present
     * in the session context during a restore.
     */
    @PublicEvolving
    public enum CatalogPlanCompilation implements DescribedEnum {
        ALL(
                text(
                        "All metadata about catalog tables, functions, or data types will "
                                + "be persisted into the plan during compilation. For catalog tables, "
                                + "this includes the table's identifier, schema, and options. For "
                                + "catalog functions, this includes the function's identifier and "
                                + "class. For catalog data types, this includes the identifier and "
                                + "entire type structure. "
                                + "With this strategy, the catalog's metadata doesn't have to be "
                                + "available anymore during a restore operation.")),
        SCHEMA(
                text(
                        "In addition to an identifier, schema information about catalog tables, "
                                + "functions, or data types will be persisted into the plan during "
                                + "compilation. A schema allows for detecting incompatible changes "
                                + "in the catalog during a plan restore operation. However, all "
                                + "other metadata will still be retrieved from the catalog.")),

        IDENTIFIER(
                text(
                        "Only the identifier of catalog tables, functions, or data types will be "
                                + "persisted into the plan during compilation. All metadata will "
                                + "be retrieved from the catalog during a restore operation. With "
                                + "this strategy, plans become less verbose."));

        private final InlineElement description;

        CatalogPlanCompilation(InlineElement description) {
            this.description = description;
        }

        @Internal
        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    /** Strategy to restore {@link Catalog} objects using the plan and lookups if necessary. */
    @PublicEvolving
    public enum CatalogPlanRestore implements DescribedEnum {
        ALL(
                text(
                        "Reads all metadata about catalog tables, functions, or data types that has "
                                + "been persisted in the plan. The strategy performs a catalog "
                                + "lookup by identifier to fill in missing information or enrich "
                                + "mutable options. If the original object is not available in the "
                                + "catalog anymore, pipelines can still be restored if all information "
                                + "necessary is contained in the plan.")),
        ALL_ENFORCED(
                text(
                        "Requires that all metadata about catalog tables, functions, or data types "
                                + "has been persisted in the plan. The strategy will neither "
                                + "perform a catalog lookup by identifier nor enrich mutable "
                                + "options with catalog information. A restore will fail if not all "
                                + "information necessary is contained in the plan.")),

        IDENTIFIER(
                text(
                        "Uses only the identifier of catalog tables, functions, or data types and "
                                + "always performs a catalog lookup. A restore will fail if the "
                                + "original object is not available in the catalog anymore. "
                                + "Additional metadata that might be contained in the plan will "
                                + "be ignored."));

        private final InlineElement description;

        CatalogPlanRestore(InlineElement description) {
            this.description = description;
        }

        @Internal
        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    /** Strategy to expand columns in {@code SELECT *} queries. */
    @PublicEvolving
    public enum ColumnExpansionStrategy implements DescribedEnum {
        EXCLUDE_ALIASED_VIRTUAL_METADATA_COLUMNS(
                text(
                        "Excludes virtual metadata columns that reference a metadata key via an alias. "
                                + "For example, a column declared as 'c METADATA VIRTUAL FROM k' "
                                + "is not selected by default if the strategy is applied.")),

        EXCLUDE_DEFAULT_VIRTUAL_METADATA_COLUMNS(
                text(
                        "Excludes virtual metadata columns that directly reference a metadata key. "
                                + "For example, a column declared as 'k METADATA VIRTUAL' "
                                + "is not selected by default if the strategy is applied."));

        private final InlineElement description;

        ColumnExpansionStrategy(InlineElement description) {
            this.description = description;
        }

        @Internal
        @Override
        public InlineElement getDescription() {
            return description;
        }
    }
}
