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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.calcite.bridge.CalciteContext;
import org.apache.flink.table.calcite.bridge.PlannerExternalQueryOperation;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogRegistry;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.module.hive.udf.generic.HiveGenericUDFToDecimal;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserDirectoryDesc;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserQB;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserSqlFunctionConverter;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserTypeConverter;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserDDLSemanticAnalyzer;
import org.apache.flink.table.planner.plan.nodes.hive.LogicalDistribution;
import org.apache.flink.table.planner.plan.nodes.hive.LogicalScriptTransform;
import org.apache.flink.table.planner.utils.TableSchemaUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.QBMetaData;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.SettableUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.catalog.hive.util.Constants.TABLE_LOCATION_URI;
import static org.apache.flink.table.planner.delegation.hive.HiveParserConstants.IS_INSERT_DIRECTORY;
import static org.apache.flink.table.planner.delegation.hive.HiveParserConstants.IS_TO_LOCAL_DIRECTORY;

/** A helper class to handle DMLs in hive dialect. */
public class HiveParserDMLHelper {

    private final CalciteContext calciteContext;
    private final SqlFunctionConverter funcConverter;
    private final CatalogRegistry catalogRegistry;

    public HiveParserDMLHelper(
            CalciteContext calciteContext,
            SqlFunctionConverter funcConverter,
            CatalogRegistry catalogRegistry) {
        this.calciteContext = calciteContext;
        this.funcConverter = funcConverter;
        this.catalogRegistry = catalogRegistry;
    }

    public Tuple4<ObjectIdentifier, QueryOperation, Map<String, String>, Boolean>
            createInsertOperationInfo(
                    RelNode queryRelNode,
                    CatalogTable destTable,
                    ObjectIdentifier destTableIdentifier,
                    Map<String, String> staticPartSpec,
                    List<String> destSchema,
                    boolean overwrite)
                    throws SemanticException {
        // sanity check
        Preconditions.checkArgument(
                queryRelNode instanceof Project
                        || queryRelNode instanceof Sort
                        || queryRelNode instanceof LogicalDistribution
                        || queryRelNode instanceof LogicalScriptTransform,
                "Expect top RelNode to be Project, Sort, LogicalDistribution, or LogicalScriptTransform, actually got "
                        + queryRelNode);
        if (!(queryRelNode instanceof Project)
                && !(queryRelNode instanceof LogicalScriptTransform)) {
            RelNode parent = ((SingleRel) queryRelNode).getInput();
            // SEL + SORT or SEL + DIST + LIMIT
            Preconditions.checkArgument(
                    parent instanceof Project
                            || parent instanceof LogicalDistribution
                            || parent instanceof LogicalScriptTransform,
                    "Expect input to be a Project or LogicalDistribution, actually got " + parent);
            if (parent instanceof LogicalDistribution) {
                RelNode grandParent = ((LogicalDistribution) parent).getInput();
                Preconditions.checkArgument(
                        grandParent instanceof Project,
                        "Expect input of LogicalDistribution to be a Project, actually got "
                                + grandParent);
            }
        }

        // handle dest schema, e.g. insert into dest(.,.,.) select ...
        queryRelNode =
                handleDestSchema(
                        (SingleRel) queryRelNode, destTable, destSchema, staticPartSpec.keySet());

        // track each target col and its expected type
        RelDataTypeFactory typeFactory = calciteContext.getTypeFactory();
        LinkedHashMap<String, RelDataType> targetColToCalcType = new LinkedHashMap<>();
        List<TypeInfo> targetHiveTypes = new ArrayList<>();
        ResolvedSchema resolvedSchema = ((ResolvedCatalogTable) destTable).getResolvedSchema();
        String[] fieldNames = resolvedSchema.getColumnNames().toArray(new String[] {});
        for (String fieldName : fieldNames) {
            Optional<DataType> dataType =
                    resolvedSchema.getColumn(fieldName).map(Column::getDataType);
            TypeInfo hiveType =
                    HiveTypeUtil.toHiveTypeInfo(
                            dataType.orElseThrow(
                                    () ->
                                            new SemanticException(
                                                    String.format(
                                                            "Can't get data type for column %s of table %s.",
                                                            fieldName,
                                                            destTableIdentifier
                                                                    .asSummaryString()))),
                            false);
            targetHiveTypes.add(hiveType);
            targetColToCalcType.put(
                    fieldName, HiveParserTypeConverter.convert(hiveType, typeFactory));
        }

        // add static partitions to query source
        if (!staticPartSpec.isEmpty()) {
            if (queryRelNode instanceof Project) {
                queryRelNode =
                        replaceProjectForStaticPart(
                                (Project) queryRelNode,
                                staticPartSpec,
                                destTable,
                                targetColToCalcType);
            } else if (queryRelNode instanceof Sort) {
                Sort sort = (Sort) queryRelNode;
                RelNode oldInput = sort.getInput();
                RelNode newInput;
                if (oldInput instanceof LogicalDistribution) {
                    newInput =
                            replaceDistForStaticParts(
                                    (LogicalDistribution) oldInput,
                                    destTable,
                                    staticPartSpec,
                                    targetColToCalcType);
                } else {
                    newInput =
                            replaceOrAddProjectForStaticPart(
                                    oldInput, staticPartSpec, destTable, targetColToCalcType);
                    // we may need to shift the field collations
                    final int numDynmPart =
                            destTable.getPartitionKeys().size() - staticPartSpec.size();
                    if (!sort.getCollation().getFieldCollations().isEmpty() && numDynmPart > 0) {
                        sort.replaceInput(0, null);
                        sort =
                                LogicalSort.create(
                                        newInput,
                                        shiftRelCollation(
                                                sort.getCollation(),
                                                (Project) oldInput,
                                                staticPartSpec.size(),
                                                numDynmPart),
                                        sort.offset,
                                        sort.fetch);
                    }
                }
                sort.replaceInput(0, newInput);
                queryRelNode = sort;
            } else if (queryRelNode instanceof LogicalDistribution) {
                queryRelNode =
                        replaceDistForStaticParts(
                                (LogicalDistribution) queryRelNode,
                                destTable,
                                staticPartSpec,
                                targetColToCalcType);
            } else {
                queryRelNode =
                        addProjectForStaticPart(
                                (LogicalScriptTransform) queryRelNode,
                                staticPartSpec,
                                destTable,
                                targetColToCalcType);
            }
        }

        // add type conversions
        queryRelNode =
                addTypeConversions(
                        calciteContext.getCluster().getRexBuilder(),
                        queryRelNode,
                        new ArrayList<>(targetColToCalcType.values()),
                        targetHiveTypes,
                        funcConverter,
                        false);

        return Tuple4.of(
                destTableIdentifier,
                new PlannerExternalQueryOperation(
                        queryRelNode, TableSchemaUtils.resolvedSchema(queryRelNode)),
                staticPartSpec,
                overwrite);
    }

    public Operation createInsertOperation(HiveParserCalcitePlanner analyzer, RelNode queryRelNode)
            throws SemanticException {
        HiveParserQB topQB = analyzer.getQB();
        HiveParserQBMetaData qbMetaData = topQB.getMetaData();
        // decide the dest table
        Map<String, Tuple2<String, CatalogTable>> nameToDestTable = qbMetaData.getNameToDestTable();
        Map<String, Tuple3<String, CatalogTable, CatalogPartitionSpec>> nameToDestPart =
                qbMetaData.getNameToDestPartition();
        // for now we only support inserting to a single table in one queryRelNode
        Preconditions.checkState(
                nameToDestTable.size() <= 1 && nameToDestPart.size() <= 1,
                "Only support inserting to 1 table");
        CatalogTable destTable;
        String insClauseName;
        String tableName;
        if (!nameToDestTable.isEmpty()) {
            insClauseName = nameToDestTable.keySet().iterator().next();
            Tuple2<String, CatalogTable> nameTable = nameToDestTable.values().iterator().next();
            tableName = nameTable.f0;
            destTable = nameTable.f1;
        } else if (!nameToDestPart.isEmpty()) {
            insClauseName = nameToDestPart.keySet().iterator().next();
            Tuple3<String, CatalogTable, CatalogPartitionSpec> nameTable =
                    nameToDestPart.values().iterator().next();
            tableName = nameTable.f0;
            destTable = nameTable.f1;
        } else {
            // happens for INSERT DIRECTORY
            return createInsertIntoDirectoryOperation(topQB, qbMetaData, queryRelNode);
        }

        // decide static partition specs
        Map<String, String> staticPartSpec = new LinkedHashMap<>();
        if (destTable.isPartitioned()) {
            List<String> partCols = destTable.getPartitionKeys();

            if (!nameToDestPart.isEmpty()) {
                // static partition
                CatalogPartitionSpec destPart = nameToDestPart.values().iterator().next().f2;
                Preconditions.checkState(
                        partCols.size() == destPart.getPartitionSpec().size(),
                        "Part cols and static spec doesn't match");
                for (String partCol : partCols) {
                    staticPartSpec.put(partCol, destPart.getPartitionSpec().get(partCol));
                }
            } else {
                // dynamic partition
                Map<String, String> spec = qbMetaData.getPartSpecForAlias(insClauseName);
                if (spec != null) {
                    for (String partCol : partCols) {
                        String val = spec.get(partCol);
                        if (val != null) {
                            staticPartSpec.put(partCol, val);
                        }
                    }
                }
            }
        }

        // decide whether it's overwrite
        boolean overwrite =
                topQB.getParseInfo().getInsertOverwriteTables().keySet().stream()
                        .map(String::toLowerCase)
                        .collect(Collectors.toSet())
                        .contains(tableName.toLowerCase());

        boolean isInsertInto = topQB.getParseInfo().isInsertIntoTable(tableName);

        Preconditions.checkArgument(
                overwrite | isInsertInto,
                "Inconsistent data structure detected: we are writing to "
                        + tableName
                        + ", but it's not in isInsertIntoTable() or getInsertOverwriteTables()."
                        + " This is a bug. Please consider filing an issue.");

        Tuple4<ObjectIdentifier, QueryOperation, Map<String, String>, Boolean> insertOperationInfo =
                createInsertOperationInfo(
                        queryRelNode,
                        destTable,
                        HiveParserBaseSemanticAnalyzer.parseCompoundName(
                                catalogRegistry, tableName),
                        staticPartSpec,
                        analyzer.getDestSchemaForClause(insClauseName),
                        overwrite);

        return new SinkModifyOperation(
                getContextResolvedTable(insertOperationInfo.f0),
                insertOperationInfo.f1,
                insertOperationInfo.f2,
                null, // targetColumns
                insertOperationInfo.f3,
                Collections.emptyMap());
    }

    private ContextResolvedTable getContextResolvedTable(ObjectIdentifier tableIdentifier) {
        ResolvedCatalogBaseTable<?> resolvedCatalogBaseTable = getTableOrError(tableIdentifier);
        ContextResolvedTable contextResolvedTable;
        if (catalogRegistry.isTemporaryTable(tableIdentifier)) {
            contextResolvedTable =
                    ContextResolvedTable.temporary(tableIdentifier, resolvedCatalogBaseTable);
        } else {
            contextResolvedTable =
                    ContextResolvedTable.permanent(
                            tableIdentifier,
                            catalogRegistry.getCatalogOrError(tableIdentifier.getCatalogName()),
                            resolvedCatalogBaseTable);
        }
        return contextResolvedTable;
    }

    private ResolvedCatalogBaseTable<?> getTableOrError(ObjectIdentifier tableIdentifier) {
        Optional<ResolvedCatalogBaseTable<?>> table =
                catalogRegistry.getCatalogBaseTable(tableIdentifier);
        return table.orElseThrow(
                () ->
                        new TableException(
                                String.format(
                                        "Cannot find table '%s' in any of the catalogs, nor as a temporary table.",
                                        tableIdentifier)));
    }

    private SinkModifyOperation createInsertIntoDirectoryOperation(
            HiveParserQB topQB, HiveParserQBMetaData qbMetaData, RelNode queryRelNode) {
        String dest = topQB.getParseInfo().getClauseNamesForDest().iterator().next();
        // get the location for insert into directory
        String location = qbMetaData.getDestFileForAlias(dest);
        // get whether it's for insert local directory
        boolean isToLocal = qbMetaData.getDestTypeForAlias(dest) == QBMetaData.DEST_LOCAL_FILE;
        HiveParserDirectoryDesc directoryDesc = topQB.getDirectoryDesc();

        // set row format / stored as / location
        Map<String, String> props = new HashMap<>();
        HiveParserDDLSemanticAnalyzer.encodeRowFormat(directoryDesc.getRowFormatParams(), props);
        HiveParserDDLSemanticAnalyzer.encodeStorageFormat(directoryDesc.getStorageFormat(), props);
        props.put(TABLE_LOCATION_URI, location);

        props.put(FactoryUtil.CONNECTOR.key(), HiveCatalogFactoryOptions.IDENTIFIER);
        // mark it's for insert into directory
        props.put(CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX + IS_INSERT_DIRECTORY, "true");
        // mark it's for insert into local directory or not
        props.put(
                CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX + IS_TO_LOCAL_DIRECTORY,
                String.valueOf(isToLocal));

        List<RelDataTypeField> fieldList = queryRelNode.getRowType().getFieldList();
        String[] colNameArr = new String[fieldList.size()];
        String[] colTypeArr = new String[fieldList.size()];
        for (int i = 0; i < fieldList.size(); i++) {
            colNameArr[i] = fieldList.get(i).getName();
            TypeInfo typeInfo = HiveParserTypeConverter.convert(fieldList.get(i).getType());
            if (typeInfo.equals(TypeInfoFactory.voidTypeInfo)) {
                colTypeArr[i] = TypeInfoFactory.stringTypeInfo.getTypeName();
            } else {
                colTypeArr[i] = typeInfo.getTypeName();
            }
        }

        // extra information needed for insert into directory
        String colNames = String.join(",", colNameArr);
        String colTypes = String.join(":", colTypeArr);
        props.put("columns", colNames);
        props.put("columns.types", colTypes);

        PlannerExternalQueryOperation plannerQueryOperation =
                new PlannerExternalQueryOperation(
                        queryRelNode, TableSchemaUtils.resolvedSchema(queryRelNode));
        return new SinkModifyOperation(
                createDummyTableForInsertDirectory(
                        plannerQueryOperation.getResolvedSchema(), props),
                plannerQueryOperation,
                Collections.emptyMap(),
                null, // targetColumns
                true, // insert into directory is always for overwrite
                Collections.emptyMap());
    }

    private ContextResolvedTable createDummyTableForInsertDirectory(
            ResolvedSchema resolvedSchema, Map<String, String> props) {
        Schema schema = Schema.newBuilder().fromResolvedSchema(resolvedSchema).build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        schema,
                        "a dummy table for the case of insert overwrite directory ",
                        Collections.emptyList(),
                        props);
        ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(catalogTable, resolvedSchema);
        String currentCatalog = catalogRegistry.getCurrentCatalog();
        // the object name means nothing, it's just for placeholder and won't be used actually
        String objectName = "insert_directory_tbl";
        return ContextResolvedTable.permanent(
                ObjectIdentifier.of(
                        currentCatalog, catalogRegistry.getCurrentDatabase(), objectName),
                catalogRegistry.getCatalogOrError(catalogRegistry.getCurrentCatalog()),
                resolvedCatalogTable);
    }

    private RelNode replaceDistForStaticParts(
            LogicalDistribution hiveDist,
            CatalogTable destTable,
            Map<String, String> staticPartSpec,
            Map<String, RelDataType> targetColToType)
            throws SemanticException {
        RelNode originInput = hiveDist.getInput();
        RelNode expandedProject =
                replaceOrAddProjectForStaticPart(
                        hiveDist.getInput(), staticPartSpec, destTable, targetColToType);
        hiveDist.replaceInput(0, null);
        final int toShift = staticPartSpec.size();
        final int numDynmPart = destTable.getPartitionKeys().size() - toShift;
        return LogicalDistribution.create(
                expandedProject,
                shiftRelCollation(hiveDist.getCollation(), originInput, toShift, numDynmPart),
                shiftDistKeys(hiveDist.getDistKeys(), originInput, toShift, numDynmPart));
    }

    private static List<Integer> shiftDistKeys(
            List<Integer> distKeys, RelNode originRelNode, int toShift, int numDynmPart) {
        List<Integer> shiftedDistKeys = new ArrayList<>(distKeys.size());
        // starting index of dynamic parts, static parts needs to be inserted before them
        final int insertIndex = originRelNode.getRowType().getFieldCount() - numDynmPart;
        for (Integer key : distKeys) {
            if (key >= insertIndex) {
                key += toShift;
            }
            shiftedDistKeys.add(key);
        }
        return shiftedDistKeys;
    }

    private RelCollation shiftRelCollation(
            RelCollation collation, RelNode originRelNode, int toShift, int numDynmPart) {
        List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
        // starting index of dynamic parts, static parts needs to be inserted before them
        final int insertIndex = originRelNode.getRowType().getFieldCount() - numDynmPart;
        List<RelFieldCollation> shiftedCollations = new ArrayList<>(fieldCollations.size());
        for (RelFieldCollation fieldCollation : fieldCollations) {
            if (fieldCollation.getFieldIndex() >= insertIndex) {
                fieldCollation =
                        fieldCollation.withFieldIndex(fieldCollation.getFieldIndex() + toShift);
            }
            shiftedCollations.add(fieldCollation);
        }
        return calciteContext.getCluster().traitSet().canonize(RelCollations.of(shiftedCollations));
    }

    static RelNode addTypeConversions(
            RexBuilder rexBuilder,
            RelNode queryRelNode,
            List<RelDataType> targetCalcTypes,
            List<TypeInfo> targetHiveTypes,
            SqlFunctionConverter funcConverter,
            boolean isSubQuery)
            throws SemanticException {
        if (isSubQuery) {
            // if it's subquery, we will skip create project in the case that the subquery and the
            // project is identity. so when it's needed to do type conversion in subquey, we add a
            // project to it directly instead of finding project in parent for we may well find not
            // project
            if (isTypeConversionNeeded(queryRelNode, targetCalcTypes)) {
                Project typeConvProject =
                        LogicalProject.create(
                                queryRelNode,
                                Collections.emptyList(),
                                rexBuilder.identityProjects(queryRelNode.getRowType()),
                                queryRelNode.getRowType());
                return replaceProjectForTypeConversion(
                        rexBuilder,
                        typeConvProject,
                        targetCalcTypes,
                        targetHiveTypes,
                        funcConverter);
            }
        } else {
            if (queryRelNode instanceof Project) {
                if (isTypeConversionNeeded(queryRelNode, targetCalcTypes)) {
                    return replaceProjectForTypeConversion(
                            rexBuilder,
                            (Project) queryRelNode,
                            targetCalcTypes,
                            targetHiveTypes,
                            funcConverter);
                }
            } else if (queryRelNode instanceof LogicalScriptTransform) {
                if (isTypeConversionNeeded(queryRelNode, targetCalcTypes)) {
                    return addProjectForTypeConversion(
                            rexBuilder,
                            (LogicalScriptTransform) queryRelNode,
                            targetCalcTypes,
                            targetHiveTypes,
                            funcConverter);
                }
            } else {
                // current node is not Project, we search for it in inputs
                RelNode newInput =
                        addTypeConversions(
                                rexBuilder,
                                queryRelNode.getInput(0),
                                targetCalcTypes,
                                targetHiveTypes,
                                funcConverter,
                                isSubQuery);
                queryRelNode.replaceInput(0, newInput);
            }
        }
        return queryRelNode;
    }

    private static RexNode createConversionCast(
            RexBuilder rexBuilder,
            RexNode srcRex,
            TypeInfo targetHiveType,
            RelDataType targetCalType,
            SqlFunctionConverter funcConverter)
            throws SemanticException {
        if (funcConverter == null) {
            return rexBuilder.makeCast(targetCalType, srcRex);
        }

        if (HiveParserUtils.isFromTimeStampToDecimal(srcRex.getType(), targetCalType)) {
            // special case for cast timestamp to decimal for Flink don't support cast from
            // TIMESTAMP type to NUMERIC type.
            // use custom to_decimal function to cast, which is consistent with Hive.
            SqlOperator castOperator =
                    HiveParserSqlFunctionConverter.getCalciteFn(
                            HiveGenericUDFToDecimal.NAME,
                            Arrays.asList(srcRex.getType(), targetCalType),
                            targetCalType,
                            false,
                            funcConverter);
            RexCall cast =
                    (RexCall)
                            rexBuilder.makeCall(
                                    castOperator,
                                    srcRex,
                                    rexBuilder.makeNullLiteral(targetCalType));
            return cast.accept(funcConverter);
        }

        // hive implements CAST with UDFs
        String udfName = TypeInfoUtils.getBaseName(targetHiveType.getTypeName());
        FunctionInfo functionInfo;
        try {
            functionInfo = FunctionRegistry.getFunctionInfo(udfName);
        } catch (SemanticException e) {
            throw new SemanticException(
                    String.format("Failed to get UDF %s for casting", udfName), e);
        }
        if (functionInfo == null || functionInfo.getGenericUDF() == null) {
            throw new SemanticException(String.format("Failed to get UDF %s for casting", udfName));
        }
        if (functionInfo.getGenericUDF() instanceof SettableUDF) {
            // For SettableUDF, we need to pass target TypeInfo to it, but we don't have a way to do
            // that currently. Therefore just use calcite cast for these types.
            return rexBuilder.makeCast(targetCalType, srcRex);
        } else {
            RexCall cast =
                    (RexCall)
                            rexBuilder.makeCall(
                                    HiveParserSqlFunctionConverter.getCalciteOperator(
                                            udfName,
                                            functionInfo.getGenericUDF(),
                                            Collections.singletonList(srcRex.getType()),
                                            targetCalType,
                                            funcConverter),
                                    srcRex);
            if (!funcConverter.hasOverloadedOp(
                    cast.getOperator(), SqlFunctionCategory.USER_DEFINED_FUNCTION)) {
                // we can't convert the cast operator, it can happen when hive module is not loaded,
                // in which case fall back to calcite cast
                return rexBuilder.makeCast(targetCalType, srcRex);
            }
            return cast.accept(funcConverter);
        }
    }

    // to check whether it's needed to do type conversion
    private static boolean isTypeConversionNeeded(
            RelNode queryRelNode, List<RelDataType> targetCalcTypes) {
        List<RelDataTypeField> fields = queryRelNode.getRowType().getFieldList();
        Preconditions.checkState(fields.size() == targetCalcTypes.size());
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getType().getSqlTypeName()
                    != targetCalcTypes.get(i).getSqlTypeName()) {
                return true;
            }
        }
        return false;
    }

    private static RelNode replaceProjectForTypeConversion(
            RexBuilder rexBuilder,
            Project project,
            List<RelDataType> targetCalcTypes,
            List<TypeInfo> targetHiveTypes,
            SqlFunctionConverter funcConverter)
            throws SemanticException {
        List<RexNode> exprs = project.getProjects();
        List<RexNode> updatedExprs = new ArrayList<>(exprs.size());
        for (int i = 0; i < exprs.size(); i++) {
            RexNode expr = exprs.get(i);
            if (expr.getType().getSqlTypeName() != targetCalcTypes.get(i).getSqlTypeName()) {
                TypeInfo hiveType = targetHiveTypes.get(i);
                RelDataType calcType = targetCalcTypes.get(i);
                // only support converting primitive types
                if (hiveType.getCategory() == ObjectInspector.Category.PRIMITIVE) {
                    expr =
                            createConversionCast(
                                    rexBuilder, expr, hiveType, calcType, funcConverter);
                }
            }
            updatedExprs.add(expr);
        }
        RelNode newProject =
                LogicalProject.create(
                        project.getInput(),
                        Collections.emptyList(),
                        updatedExprs,
                        getProjectNames(project));
        project.replaceInput(0, null);
        return newProject;
    }

    private static RelNode addProjectForTypeConversion(
            RexBuilder rexBuilder,
            LogicalScriptTransform scriptTransform,
            List<RelDataType> targetCalcTypes,
            List<TypeInfo> targetHiveTypes,
            SqlFunctionConverter funcConverter)
            throws SemanticException {
        List<RelDataTypeField> outPutDataTypes = scriptTransform.getRowType().getFieldList();
        List<RexNode> updatedExprs = new ArrayList<>(outPutDataTypes.size());
        for (int i = 0; i < outPutDataTypes.size(); i++) {
            RelDataTypeField outputDataType = outPutDataTypes.get(i);
            RexNode expr = rexBuilder.makeInputRef(scriptTransform, i);
            if (outputDataType.getType().getSqlTypeName()
                    != targetCalcTypes.get(i).getSqlTypeName()) {
                TypeInfo hiveType = targetHiveTypes.get(i);
                RelDataType calcType = targetCalcTypes.get(i);
                // only support converting primitive types
                if (hiveType.getCategory() == ObjectInspector.Category.PRIMITIVE) {
                    expr =
                            createConversionCast(
                                    rexBuilder, expr, hiveType, calcType, funcConverter);
                }
            }
            updatedExprs.add(expr);
        }
        return LogicalProject.create(
                scriptTransform, Collections.emptyList(), updatedExprs, (List<String>) null);
    }

    private RelNode handleDestSchema(
            SingleRel queryRelNode,
            CatalogTable destTable,
            List<String> destSchema,
            Set<String> staticParts)
            throws SemanticException {
        if (destSchema == null || destSchema.isEmpty()) {
            return queryRelNode;
        }

        ResolvedCatalogTable resolvedCatalogTable = (ResolvedCatalogTable) destTable;
        // natural schema should contain regular cols + dynamic cols
        ResolvedSchema resolvedSchema = resolvedCatalogTable.getResolvedSchema();
        List<String> naturalSchema = new ArrayList<>();
        for (String fieldName : resolvedSchema.getColumnNames()) {
            // only add no partition cols and dynamic partition cols
            if (!staticParts.contains(fieldName)) {
                naturalSchema.add(fieldName);
            }
        }
        // we don't need to do anything if the dest schema is the same as natural schema
        if (destSchema.equals(naturalSchema)) {
            return queryRelNode;
        }
        // build a list to create a Project on top of original Project
        // for each col in dest table, if it's in dest schema, store its corresponding index in the
        // dest schema, otherwise store its type and we'll create NULL for it
        List<Object> updatedIndices = new ArrayList<>(naturalSchema.size());
        for (String col : naturalSchema) {
            int index = destSchema.indexOf(col);
            if (index < 0) {
                Optional<DataType> dataType =
                        resolvedSchema.getColumn(col).map(Column::getDataType);
                TypeInfo hiveType =
                        HiveTypeUtil.toHiveTypeInfo(
                                dataType.orElseThrow(
                                        () ->
                                                new SemanticException(
                                                        String.format(
                                                                "Can't get data type for column %s.",
                                                                col))),
                                false);
                updatedIndices.add(
                        HiveParserTypeConverter.convert(hiveType, calciteContext.getTypeFactory()));
            } else {
                updatedIndices.add(index);
            }
        }
        if (queryRelNode instanceof Project) {
            return addProjectForDestSchema(queryRelNode, updatedIndices);
        } else if (queryRelNode instanceof Sort) {
            Sort sort = (Sort) queryRelNode;
            RelNode sortInput = sort.getInput();
            // DIST + LIMIT
            if (sortInput instanceof LogicalDistribution) {
                RelNode newDist =
                        handleDestSchemaForDist((LogicalDistribution) sortInput, updatedIndices);
                sort.replaceInput(0, newDist);
                return sort;
            }
            // PROJECT + SORT or SCRIPT_TRANSFORM + SORT
            RelNode addedProject = addProjectForDestSchema(sortInput, updatedIndices);
            // we may need to update the field collations
            List<RelFieldCollation> fieldCollations = sort.getCollation().getFieldCollations();
            if (!fieldCollations.isEmpty()) {
                sort.replaceInput(0, null);
                sort =
                        LogicalSort.create(
                                addedProject,
                                updateRelCollation(sort.getCollation(), updatedIndices),
                                sort.offset,
                                sort.fetch);
            }
            sort.replaceInput(0, addedProject);
            return sort;
        } else if (queryRelNode instanceof LogicalDistribution) {
            return handleDestSchemaForDist((LogicalDistribution) queryRelNode, updatedIndices);
        } else {
            // SCRIPT_TRANSFORM
            return handleDestSchemaForScriptTransform(
                    (LogicalScriptTransform) queryRelNode, updatedIndices);
        }
    }

    private RelNode handleDestSchemaForDist(
            LogicalDistribution hiveDist, List<Object> updatedIndices) throws SemanticException {
        RelNode addedProject = addProjectForDestSchema(hiveDist.getInput(), updatedIndices);
        // disconnect the original LogicalDistribution
        hiveDist.replaceInput(0, null);
        return LogicalDistribution.create(
                addedProject,
                updateRelCollation(hiveDist.getCollation(), updatedIndices),
                updateDistKeys(hiveDist.getDistKeys(), updatedIndices));
    }

    private RelNode handleDestSchemaForScriptTransform(
            LogicalScriptTransform hiveScripTfm, List<Object> updatedIndices)
            throws SemanticException {
        return addProjectForDestSchema(
                hiveScripTfm, hiveScripTfm.getRowType().getFieldCount(), updatedIndices);
    }

    private RelCollation updateRelCollation(RelCollation collation, List<Object> updatedIndices) {
        List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
        if (fieldCollations.isEmpty()) {
            return collation;
        }
        List<RelFieldCollation> updatedCollations = new ArrayList<>(fieldCollations.size());
        for (RelFieldCollation fieldCollation : fieldCollations) {
            int newIndex = updatedIndices.indexOf(fieldCollation.getFieldIndex());
            Preconditions.checkState(newIndex >= 0, "Sort/Order references a non-existing field");
            fieldCollation = fieldCollation.withFieldIndex(newIndex);
            updatedCollations.add(fieldCollation);
        }
        return calciteContext.getCluster().traitSet().canonize(RelCollations.of(updatedCollations));
    }

    private List<Integer> updateDistKeys(List<Integer> distKeys, List<Object> updatedIndices) {
        List<Integer> updatedDistKeys = new ArrayList<>(distKeys.size());
        for (Integer key : distKeys) {
            int newKey = updatedIndices.indexOf(key);
            Preconditions.checkState(
                    newKey >= 0, "Cluster/Distribute references a non-existing field");
            updatedDistKeys.add(newKey);
        }
        return updatedDistKeys;
    }

    private RelNode replaceOrAddProjectForStaticPart(
            RelNode relNode,
            Map<String, String> staticPartSpec,
            CatalogTable destTable,
            Map<String, RelDataType> targetColToType)
            throws SemanticException {
        if (relNode instanceof Project) {
            // replace Project
            return replaceProjectForStaticPart(
                    (Project) relNode, staticPartSpec, destTable, targetColToType);
        } else if (relNode instanceof LogicalScriptTransform) {
            //  add Project
            return addProjectForStaticPart(
                    (LogicalScriptTransform) relNode, staticPartSpec, destTable, targetColToType);
        } else {
            throw new SemanticException(
                    "Expect input to be a Project or LogicalScriptTransform, actually got "
                            + relNode);
        }
    }

    private RelNode replaceProjectForStaticPart(
            Project project,
            Map<String, String> staticPartSpec,
            CatalogTable destTable,
            Map<String, RelDataType> targetColToType) {
        List<RexNode> extendedExprs =
                addExtraRexNodeForStaticPart(
                        project.getProjects(), staticPartSpec, destTable, targetColToType);
        RelNode res =
                LogicalProject.create(
                        project.getInput(),
                        Collections.emptyList(),
                        extendedExprs,
                        (List<String>) null);
        project.replaceInput(0, null);
        return res;
    }

    private Project addProjectForStaticPart(
            LogicalScriptTransform logicalScriptTransform,
            Map<String, String> staticPartSpec,
            CatalogTable destTable,
            Map<String, RelDataType> targetColToType) {
        RexBuilder rexBuilder = calciteContext.getCluster().getRexBuilder();
        List<RexNode> originRexNodes =
                IntStream.range(0, logicalScriptTransform.getRowType().getFieldCount())
                        .mapToObj(i -> rexBuilder.makeInputRef(logicalScriptTransform, i))
                        .collect(Collectors.toList());
        List<RexNode> finalRexNodes =
                addExtraRexNodeForStaticPart(
                        originRexNodes, staticPartSpec, destTable, targetColToType);
        return LogicalProject.create(
                logicalScriptTransform,
                Collections.emptyList(),
                finalRexNodes,
                (List<String>) null);
    }

    private List<RexNode> addExtraRexNodeForStaticPart(
            List<RexNode> originRexNodes,
            Map<String, String> staticPartSpec,
            CatalogTable destTable,
            Map<String, RelDataType> targetColToType) {
        List<RexNode> extendedRexNodes = new ArrayList<>(originRexNodes);
        RexBuilder rexBuilder = calciteContext.getCluster().getRexBuilder();
        int numDynmPart = destTable.getPartitionKeys().size() - staticPartSpec.size();
        int insertIndex = originRexNodes.size() - numDynmPart;
        for (Map.Entry<String, String> spec : staticPartSpec.entrySet()) {
            RexNode toAdd =
                    rexBuilder.makeCharLiteral(HiveParserUtils.asUnicodeString(spec.getValue()));
            toAdd = rexBuilder.makeAbstractCast(targetColToType.get(spec.getKey()), toAdd);
            extendedRexNodes.add(insertIndex++, toAdd);
        }
        return extendedRexNodes;
    }

    private static List<String> getProjectNames(Project project) {
        return project.getNamedProjects().stream().map(p -> p.right).collect(Collectors.toList());
    }

    private RelNode addProjectForDestSchema(RelNode input, List<Object> updatedIndices)
            throws SemanticException {
        if (input instanceof Project) {
            return addProjectForDestSchema(
                    input, ((Project) input).getProjects().size(), updatedIndices);
        } else if (input instanceof LogicalScriptTransform) {
            return addProjectForDestSchema(
                    input, input.getRowType().getFieldCount(), updatedIndices);
        } else {
            throw new SemanticException(
                    "Expect top RelNode to be Project, or LogicalScriptTransform when add Project for dest schema, but actually got "
                            + input);
        }
    }

    private RelNode addProjectForDestSchema(
            RelNode input, int selectSize, List<Object> updatedIndices) throws SemanticException {
        int destSchemaSize =
                (int) updatedIndices.stream().filter(o -> o instanceof Integer).count();
        if (destSchemaSize != selectSize) {
            throw new SemanticException(
                    String.format(
                            "Expected %d columns, but SEL produces %d columns",
                            destSchemaSize, selectSize));
        }
        List<RexNode> exprs = new ArrayList<>(updatedIndices.size());
        RexBuilder rexBuilder = calciteContext.getCluster().getRexBuilder();
        for (Object object : updatedIndices) {
            if (object instanceof Integer) {
                exprs.add(rexBuilder.makeInputRef(input, (Integer) object));
            } else {
                // it's ok to call calcite to do the cast since all we cast here are nulls
                RexNode rexNode = rexBuilder.makeNullLiteral((RelDataType) object);
                exprs.add(rexNode);
            }
        }
        return LogicalProject.create(input, Collections.emptyList(), exprs, (List<String>) null);
    }
}
