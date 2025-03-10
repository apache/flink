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

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter.ConvertContext;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Registry of SqlNode converters. */
public class SqlNodeConverters {

    private static final Map<Class<?>, SqlNodeConverter<?>> CLASS_CONVERTERS = new HashMap<>();
    private static final Map<SqlKind, SqlNodeConverter<?>> SQLKIND_CONVERTERS = new HashMap<>();

    static {
        // register all the converters here
        register(new SqlCreateCatalogConverter());
        register(new SqlAlterCatalogOptionsConverter());
        register(new SqlAlterCatalogResetConverter());
        register(new SqlAlterCatalogCommentConverter());
        register(new SqlCreateViewConverter());
        register(new SqlAlterViewRenameConverter());
        register(new SqlAlterViewPropertiesConverter());
        register(new SqlAlterViewAsConverter());
        register(new SqlAlterTableAddPartitionConverter());
        register(new SqlAlterTableDropPartitionConverter());
        register(new SqlQueryConverter());
        register(new SqlShowPartitionsConverter());
        register(new SqlTruncateTableConverter());
        register(new SqlShowFunctionsConverter());
        register(new SqlShowProcedureConverter());
        register(new SqlReplaceTableAsConverter());
        register(new SqlProcedureCallConverter());
        register(new SqlShowDatabasesConverter());
        register(new SqlShowCreateCatalogConverter());
        register(new SqlDescribeCatalogConverter());
        register(new SqlDescribeJobConverter());
        register(new SqlCreateMaterializedTableConverter());
        register(new SqlAlterMaterializedTableRefreshConverter());
        register(new SqlAlterMaterializedTableSuspendConverter());
        register(new SqlAlterMaterializedTableResumeConverter());
        register(new SqlAlterMaterializedTableAsQueryConverter());
        register(new SqlDropMaterializedTableConverter());
        register(new SqlShowTablesConverter());
        register(new SqlShowViewsConverter());
        register(new SqlShowCatalogsConverter());
        register(new SqlDescribeFunctionConverter());
        register(new SqlDropCatalogConverter());
        register(new SqlLoadModuleConverter());
        register(new SqlShowCurrentCatalogConverter());
        register(new SqlShowModulesConverter());
        register(new SqlUnloadModuleConverter());
        register(new SqlUseCatalogConverter());
        register(new SqlUseModulesConverter());
        register(new SqlCreateDatabaseConverter());
        register(new SqlDropDatabaseConverter());
        register(new SqlAlterDatabaseConverter());
        register(new SqlShowCurrentDatabaseConverter());
        register(new SqlUseDatabaseConverter());
        register(new SqlCreateTableConverter());
        register(new SqlDropTableConverter());
        register(new SqlAlterTableConverter());
        register(new SqlShowColumnsConverter());
        register(new SqlDropViewConverter());
        register(new SqlCreateFunctionConverter());
        register(new SqlDropFunctionConverter());
        register(new SqlAlterFunctionConverter());
        register(new SqlShowCreateTableConverter());
        register(new SqlShowCreateViewConverter());
        register(new SqlRichExplainConverter());
        register(new SqlRichDescribeTableConverter());
        register(new SqlAddJarConverter());
        register(new SqlRemoveJarConverter());
        register(new SqlShowJarsConverter());
        register(new SqlShowJobsConverter());
        register(new RichSqlInsertConverter());
        register(new SqlBeginStatementSetConverter());
        register(new SqlEndStatementSetConverter());
        register(new SqlSetConverter());
        register(new SqlResetConverter());
        register(new SqlStatementSetConverter());
        register(new SqlExecuteConverter());
        register(new SqlExecutePlanConverter());
        register(new SqlCompilePlanConverter());
        register(new SqlCompileAndExecutePlanConverter());
        register(new SqlAnalyzeTableConverter());
        register(new SqlStopJobConverter());
        register(new SqlDeleteConverter());
        register(new SqlUpdateConverter());
    }

    /**
     * Convert the given validated SqlNode into Operation if there is a registered converter for the
     * node.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Optional<Operation> convertSqlNode(
            SqlNode validatedSqlNode, ConvertContext context) {
        // match by class first
        SqlNodeConverter classConverter = CLASS_CONVERTERS.get(validatedSqlNode.getClass());
        if (classConverter != null) {
            return Optional.of(classConverter.convertSqlNode(validatedSqlNode, context));
        }
        // try to match by super classes
        Class<?> superClass = validatedSqlNode.getClass().getSuperclass();
        while (superClass != null) {
            classConverter = CLASS_CONVERTERS.get(superClass);
            if (classConverter != null) {
                return Optional.of(classConverter.convertSqlNode(validatedSqlNode, context));
            }
            superClass = superClass.getSuperclass();
        }
        // match by kind if no matching items in class converters
        SqlNodeConverter sqlKindConverter = SQLKIND_CONVERTERS.get(validatedSqlNode.getKind());
        if (sqlKindConverter != null) {
            return Optional.of(sqlKindConverter.convertSqlNode(validatedSqlNode, context));
        } else {
            return Optional.empty();
        }
    }

    private static void register(SqlNodeConverter<?> converter) {
        // register by SqlKind if it is defined
        if (converter.supportedSqlKinds().isPresent()) {
            for (SqlKind sqlKind : converter.supportedSqlKinds().get()) {
                if (SQLKIND_CONVERTERS.containsKey(sqlKind)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Failed to register converter for '%s', because there is a "
                                            + "registered converter for the SqlKind '%s'",
                                    converter.getClass().getCanonicalName(), sqlKind));
                } else {
                    SQLKIND_CONVERTERS.put(sqlKind, converter);
                }
            }
            return;
        }

        // extract the parameter type of the converter class
        TypeInformation<?> typeInfo =
                TypeExtractor.createTypeInfo(
                        converter, SqlNodeConverter.class, converter.getClass(), 0);
        Class<?> nodeClass = typeInfo.getTypeClass();
        if (CLASS_CONVERTERS.containsKey(nodeClass)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Failed to register converter for '%s', because there is a "
                                    + "registered converter for the SqlNode '%s'",
                            converter.getClass().getCanonicalName(), nodeClass.getCanonicalName()));
        } else {
            CLASS_CONVERTERS.put(nodeClass, converter);
        }
    }
}
