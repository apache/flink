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

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserQB;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserSqlFunctionConverter;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserTypeConverter;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.plan.nodes.hive.LogicalDistribution;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.QBMetaData;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.SettableUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.delegation.hive.HiveParserUtils.getProjsFromBelowAsInputRef;
import static org.apache.flink.table.planner.delegation.hive.HiveParserUtils.getSessionHiveShim;

/** A helper class to handle DMLs in hive dialect. */
public class HiveParserDMLHelper {

    private final PlannerContext plannerContext;
    private final SqlFunctionConverter funcConverter;
    private final CatalogManager catalogManager;

    public HiveParserDMLHelper(
            PlannerContext plannerContext,
            SqlFunctionConverter funcConverter,
            CatalogManager catalogManager) {
        this.plannerContext = plannerContext;
        this.funcConverter = funcConverter;
        this.catalogManager = catalogManager;
    }

    public Tuple4<ObjectIdentifier, QueryOperation, Map<String, String>, Boolean>
            createInsertOperationInfo(
                    RelNode queryRelNode,
                    Table destTable,
                    Map<String, String> staticPartSpec,
                    List<String> destSchema,
                    boolean overwrite)
                    throws SemanticException {
        // sanity check
        Preconditions.checkArgument(
                queryRelNode instanceof Project
                        || queryRelNode instanceof Sort
                        || queryRelNode instanceof LogicalDistribution,
                "Expect top RelNode to be Project, Sort, or LogicalDistribution, actually got "
                        + queryRelNode);
        if (!(queryRelNode instanceof Project)) {
            RelNode parent = ((SingleRel) queryRelNode).getInput();
            // SEL + SORT or SEL + DIST + LIMIT
            Preconditions.checkArgument(
                    parent instanceof Project || parent instanceof LogicalDistribution,
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
        RelDataTypeFactory typeFactory = plannerContext.getTypeFactory();
        LinkedHashMap<String, RelDataType> targetColToCalcType = new LinkedHashMap<>();
        List<TypeInfo> targetHiveTypes = new ArrayList<>();
        List<FieldSchema> allCols = new ArrayList<>(destTable.getCols());
        allCols.addAll(destTable.getPartCols());
        for (FieldSchema col : allCols) {
            TypeInfo hiveType = TypeInfoUtils.getTypeInfoFromTypeString(col.getType());
            targetHiveTypes.add(hiveType);
            targetColToCalcType.put(
                    col.getName(), HiveParserTypeConverter.convert(hiveType, typeFactory));
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
                            replaceProjectForStaticPart(
                                    (Project) oldInput,
                                    staticPartSpec,
                                    destTable,
                                    targetColToCalcType);
                    // we may need to shift the field collations
                    final int numDynmPart =
                            destTable.getTTable().getPartitionKeys().size() - staticPartSpec.size();
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
            } else {
                queryRelNode =
                        replaceDistForStaticParts(
                                (LogicalDistribution) queryRelNode,
                                destTable,
                                staticPartSpec,
                                targetColToCalcType);
            }
        }

        // add distribute by + sort for bucket table
        if (destTable.getNumBuckets() > 0) {
            // the targetCols is with the order of columns to be inserted
            List<String> targetCols = new ArrayList<>();
            for (FieldSchema fieldSchema : destTable.getAllCols()) {
                targetCols.add(fieldSchema.getName());
            }
            List<String> dynamicPartitionCols = new ArrayList<>();
            Set<String> staticPartitionCols = staticPartSpec.keySet();
            for (FieldSchema fieldSchema : destTable.getPartCols()) {
                if (!staticPartitionCols.contains(fieldSchema.getName())) {
                    dynamicPartitionCols.add(fieldSchema.getName());
                }
            }
            queryRelNode =
                    handleInsertIntoBucketedTable(
                            destTable, queryRelNode, dynamicPartitionCols, targetCols);
        }

        // add type conversions
        queryRelNode =
                addTypeConversions(
                        plannerContext.getCluster().getRexBuilder(),
                        queryRelNode,
                        new ArrayList<>(targetColToCalcType.values()),
                        targetHiveTypes,
                        funcConverter);

        // create identifier
        List<String> targetTablePath =
                Arrays.asList(destTable.getDbName(), destTable.getTableName());
        UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(targetTablePath);
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        return Tuple4.of(
                identifier, new PlannerQueryOperation(queryRelNode), staticPartSpec, overwrite);
    }

    /**
     * Handle the case that inserts data into a bucketed table. For bucketed table, the data will be
     * clustered by the bucketed columns and sorted by the sorted columns of the bucketed table.
     * Clustered by bucketed columns means the data with same bucket id which is calculated
     * according to bucketed columns will be written to same bucket file.
     *
     * <p>So, This method is used to add LogicalDistribution node to make data is clustered by and
     * sorted by.
     *
     * <p>Note: to make sure the bucket id is compatible to Hive, we delegate Hive's logical to
     * calculate the bucket id.
     */
    private RelNode handleInsertIntoBucketedTable(
            Table table,
            RelNode queryRelNode,
            List<String> dynamicPartitionCols,
            List<String> targetCols)
            throws SemanticException {
        // for dist/sort RelNode, we add a project node firstly
        if (queryRelNode instanceof LogicalDistribution || queryRelNode instanceof LogicalSort) {
            // first we find the first Project in the input of dist/sort RelNode
            RelNode firstProjectRelNode = queryRelNode.getInput(0);
            while (!(firstProjectRelNode instanceof Project)) {
                // search for Project in the inputs
                firstProjectRelNode = firstProjectRelNode.getInput(0);
            }
            // add a project with RexNodes of the first project found
            queryRelNode =
                    LogicalProject.create(
                            queryRelNode,
                            Collections.emptyList(),
                            ((Project) firstProjectRelNode).getProjects(),
                            (List<String>) null);
        }

        // add a project with an extra RexNode  that calculate hash code of bucket columns
        Project project = ((Project) queryRelNode);
        // get origin rexNodes
        List<RexNode> exprs = project.getProjects();
        // create an extra RexNode that call Hive's hash function
        List<RexNode> bucketColExprs = new ArrayList<>();
        List<RelDataType> bucketColRelTypes = new ArrayList<>();
        List<String> bucketCols = table.getBucketCols();
        // find the RexNodes that represent for the bucket columns
        for (String bucketCol : bucketCols) {
            int bucketColIndex = targetCols.indexOf(bucketCol);
            if (bucketColIndex < 0) {
                throw new SemanticException(
                        String.format(
                                "Can't find bucket column '%s' in the '%s' to be inserted into bucketed table %s.",
                                bucketCol, targetCols, table.getCompleteName()));
            }
            bucketColExprs.add(exprs.get(bucketColIndex));
            bucketColRelTypes.add(exprs.get(bucketColIndex).getType());
        }

        // add a RexNode to calculate bucket id
        RexNode bucketId =
                rexNodeForBucketId(table.getNumBuckets(), bucketColExprs, bucketColRelTypes);
        List<RexNode> extendedExprs = new ArrayList<>(exprs);
        extendedExprs.add(bucketId);
        // create a new Project with origin RexNodes and a RexNode for calculating bucket id
        RelNode extendProject =
                LogicalProject.create(
                        project.getInput(),
                        Collections.emptyList(),
                        extendedExprs,
                        (List<String>) null);

        // sorted by dynamic partition columns + bucket id + ordered columns
        List<RelFieldCollation> fieldCollations = new ArrayList<>();
        // first sort by dynamic partition columns
        for (String col : dynamicPartitionCols) {
            int fieldIndex = targetCols.indexOf(col);
            if (fieldIndex < 0) {
                throw new SemanticException(
                        String.format(
                                "Can't find dynamic partition column '%s' in the '%s' to be inserted into bucketed table %s.",
                                col, targetCols, table.getCompleteName()));
            }
            fieldCollations.add(
                    new RelFieldCollation(
                            fieldIndex,
                            RelFieldCollation.Direction.ASCENDING,
                            RelFieldCollation.NullDirection.FIRST));
        }

        // then sort by bucket id, which is the last expression
        fieldCollations.add(
                new RelFieldCollation(
                        extendedExprs.size() - 1,
                        RelFieldCollation.Direction.ASCENDING,
                        RelFieldCollation.NullDirection.FIRST));

        // finally, sort by ordered columns
        for (Order order : table.getSortCols()) {
            int fieldIndex = targetCols.indexOf(order.getCol());
            if (fieldIndex < 0) {
                throw new SemanticException(
                        String.format(
                                "Can't find sort column '%s' in the dest bucketed table %s column list '%s'.",
                                order.getCol(), table.getCompleteName(), targetCols));
            }
            RelFieldCollation.Direction direction;
            RelFieldCollation.NullDirection nullDirection;
            if (order.getOrder() == 1) {
                // asc
                direction = RelFieldCollation.Direction.ASCENDING;
                // only null as first is supported when sort direction is asc
                nullDirection = RelFieldCollation.NullDirection.FIRST;
            } else if (order.getOrder() == 0) {
                // desc
                direction = RelFieldCollation.Direction.DESCENDING;
                // only null as last is supported when sort direction is desc
                nullDirection = RelFieldCollation.NullDirection.LAST;
            } else {
                throw new SemanticException(
                        String.format(
                                "Unknown order %d for column %s.",
                                order.getOrder(), order.getCol()));
            }
            fieldCollations.add(new RelFieldCollation(fieldIndex, direction, nullDirection));
        }

        RelCollation canonizedCollation =
                plannerContext
                        .getCluster()
                        .traitSet()
                        .canonize(RelCollationImpl.of(fieldCollations));

        // distribute by bucket id, which is the last expression
        queryRelNode =
                LogicalDistribution.create(
                        extendProject,
                        canonizedCollation,
                        Collections.singletonList(extendedExprs.size() - 1));

        // after distributed by and sort, we should remove the last bucket_id RexNode
        List<RexNode> newRexNodes = getProjsFromBelowAsInputRef(queryRelNode);
        newRexNodes.remove(newRexNodes.size() - 1);
        return LogicalProject.create(
                queryRelNode, Collections.emptyList(), newRexNodes, (List<String>) null);
    }

    /** bucket id = bit_and(hash(bucket_columns), Int.Max) % number_of_buckets. */
    private RexNode rexNodeForBucketId(
            int numberOfBuckets, List<RexNode> bucketColExprs, List<RelDataType> bucketColRelTypes)
            throws SemanticException {
        String hashFuncName = getSessionHiveShim().getHashFuncName();
        FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(hashFuncName);
        RexBuilder rexBuilder = plannerContext.getCluster().getRexBuilder();
        RelDataType intRelDataType =
                plannerContext.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
        // hash(bucket_columns)
        RexNode hashCodeRexNode =
                rexBuilder.makeCall(
                        HiveParserSqlFunctionConverter.getCalciteOperator(
                                hashFuncName,
                                functionInfo.getGenericUDF(),
                                bucketColRelTypes,
                                intRelDataType),
                        bucketColExprs);
        if (!funcConverter.hasOverloadedOp(
                ((RexCall) hashCodeRexNode).getOperator(),
                SqlFunctionCategory.USER_DEFINED_FUNCTION)) {
            // hive module is not loaded, thus we can't find the Hive's hash function,
            // so, throw exception directly
            throw new SemanticException(
                    "Please load Hive module firstly when insert data into a bucketed table.");
        }

        hashCodeRexNode = hashCodeRexNode.accept(funcConverter);

        // bit_and(hash(bucket_columns), Int.Max)
        functionInfo = FunctionRegistry.getFunctionInfo("&");
        RexNode normalizedHashCodeRexNode =
                rexBuilder.makeCall(
                        HiveParserSqlFunctionConverter.getCalciteOperator(
                                "&",
                                functionInfo.getGenericUDF(),
                                Arrays.asList(intRelDataType, intRelDataType),
                                intRelDataType),
                        hashCodeRexNode,
                        rexBuilder.makeLiteral(Integer.MAX_VALUE, intRelDataType, true));
        normalizedHashCodeRexNode = normalizedHashCodeRexNode.accept(funcConverter);

        // bit_and(hash(bucket_columns), Int.Max) % number_of_buckets
        return rexBuilder.makeCall(
                SqlStdOperatorTable.MOD,
                normalizedHashCodeRexNode,
                rexBuilder.makeLiteral(numberOfBuckets, intRelDataType, true));
    }

    public Operation createInsertOperation(HiveParserCalcitePlanner analyzer, RelNode queryRelNode)
            throws SemanticException {
        HiveParserQB topQB = analyzer.getQB();
        QBMetaData qbMetaData = topQB.getMetaData();
        // decide the dest table
        Map<String, Table> nameToDestTable = qbMetaData.getNameToDestTable();
        Map<String, Partition> nameToDestPart = qbMetaData.getNameToDestPartition();
        // for now we only support inserting to a single table
        Preconditions.checkState(
                nameToDestTable.size() <= 1 && nameToDestPart.size() <= 1,
                "Only support inserting to 1 table");
        Table destTable;
        String insClauseName;
        if (!nameToDestTable.isEmpty()) {
            insClauseName = nameToDestTable.keySet().iterator().next();
            destTable = nameToDestTable.values().iterator().next();
        } else if (!nameToDestPart.isEmpty()) {
            insClauseName = nameToDestPart.keySet().iterator().next();
            destTable = nameToDestPart.values().iterator().next().getTable();
        } else {
            // happens for INSERT DIRECTORY
            throw new SemanticException("INSERT DIRECTORY is not supported");
        }

        // decide static partition specs
        Map<String, String> staticPartSpec = new LinkedHashMap<>();
        if (destTable.isPartitioned()) {
            List<String> partCols =
                    HiveCatalog.getFieldNames(destTable.getTTable().getPartitionKeys());

            if (!nameToDestPart.isEmpty()) {
                // static partition
                Partition destPart = nameToDestPart.values().iterator().next();
                Preconditions.checkState(
                        partCols.size() == destPart.getValues().size(),
                        "Part cols and static spec doesn't match");
                for (int i = 0; i < partCols.size(); i++) {
                    staticPartSpec.put(partCols.get(i), destPart.getValues().get(i));
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
                        .contains(destTable.getDbName() + "." + destTable.getTableName());

        Tuple4<ObjectIdentifier, QueryOperation, Map<String, String>, Boolean> insertOperationInfo =
                createInsertOperationInfo(
                        queryRelNode,
                        destTable,
                        staticPartSpec,
                        analyzer.getDestSchemaForClause(insClauseName),
                        overwrite);

        return new SinkModifyOperation(
                catalogManager.getTableOrError(insertOperationInfo.f0),
                insertOperationInfo.f1,
                insertOperationInfo.f2,
                insertOperationInfo.f3,
                Collections.emptyMap());
    }

    private RelNode replaceDistForStaticParts(
            LogicalDistribution hiveDist,
            Table destTable,
            Map<String, String> staticPartSpec,
            Map<String, RelDataType> targetColToType) {
        Project project = (Project) hiveDist.getInput();
        RelNode expandedProject =
                replaceProjectForStaticPart(project, staticPartSpec, destTable, targetColToType);
        hiveDist.replaceInput(0, null);
        final int toShift = staticPartSpec.size();
        final int numDynmPart = destTable.getTTable().getPartitionKeys().size() - toShift;
        return LogicalDistribution.create(
                expandedProject,
                shiftRelCollation(hiveDist.getCollation(), project, toShift, numDynmPart),
                shiftDistKeys(hiveDist.getDistKeys(), project, toShift, numDynmPart));
    }

    private static List<Integer> shiftDistKeys(
            List<Integer> distKeys, Project origProject, int toShift, int numDynmPart) {
        List<Integer> shiftedDistKeys = new ArrayList<>(distKeys.size());
        // starting index of dynamic parts, static parts needs to be inserted before them
        final int insertIndex = origProject.getProjects().size() - numDynmPart;
        for (Integer key : distKeys) {
            if (key >= insertIndex) {
                key += toShift;
            }
            shiftedDistKeys.add(key);
        }
        return shiftedDistKeys;
    }

    private RelCollation shiftRelCollation(
            RelCollation collation, Project origProject, int toShift, int numDynmPart) {
        List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
        // starting index of dynamic parts, static parts needs to be inserted before them
        final int insertIndex = origProject.getProjects().size() - numDynmPart;
        List<RelFieldCollation> shiftedCollations = new ArrayList<>(fieldCollations.size());
        for (RelFieldCollation fieldCollation : fieldCollations) {
            if (fieldCollation.getFieldIndex() >= insertIndex) {
                fieldCollation =
                        fieldCollation.withFieldIndex(fieldCollation.getFieldIndex() + toShift);
            }
            shiftedCollations.add(fieldCollation);
        }
        return plannerContext
                .getCluster()
                .traitSet()
                .canonize(RelCollationImpl.of(shiftedCollations));
    }

    static RelNode addTypeConversions(
            RexBuilder rexBuilder,
            RelNode queryRelNode,
            List<RelDataType> targetCalcTypes,
            List<TypeInfo> targetHiveTypes,
            SqlFunctionConverter funcConverter)
            throws SemanticException {
        if (queryRelNode instanceof Project) {
            return replaceProjectForTypeConversion(
                    rexBuilder,
                    (Project) queryRelNode,
                    targetCalcTypes,
                    targetHiveTypes,
                    funcConverter);
        } else {
            // current node is not Project, we search for it in inputs
            RelNode newInput =
                    addTypeConversions(
                            rexBuilder,
                            queryRelNode.getInput(0),
                            targetCalcTypes,
                            targetHiveTypes,
                            funcConverter);
            queryRelNode.replaceInput(0, newInput);
            return queryRelNode;
        }
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
                                            targetCalType),
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

    private static RelNode replaceProjectForTypeConversion(
            RexBuilder rexBuilder,
            Project project,
            List<RelDataType> targetCalcTypes,
            List<TypeInfo> targetHiveTypes,
            SqlFunctionConverter funcConverter)
            throws SemanticException {
        List<RexNode> exprs = project.getProjects();
        Preconditions.checkState(
                exprs.size() == targetCalcTypes.size(),
                "Expressions and target types size mismatch");
        List<RexNode> updatedExprs = new ArrayList<>(exprs.size());
        boolean updated = false;
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
                    updated = true;
                }
            }
            updatedExprs.add(expr);
        }
        if (updated) {
            RelNode newProject =
                    LogicalProject.create(
                            project.getInput(),
                            Collections.emptyList(),
                            updatedExprs,
                            getProjectNames(project));
            project.replaceInput(0, null);
            return newProject;
        } else {
            return project;
        }
    }

    private RelNode handleDestSchema(
            SingleRel queryRelNode,
            Table destTable,
            List<String> destSchema,
            Set<String> staticParts)
            throws SemanticException {
        if (destSchema == null || destSchema.isEmpty()) {
            return queryRelNode;
        }

        // natural schema should contain regular cols + dynamic cols
        List<FieldSchema> naturalSchema = new ArrayList<>(destTable.getCols());
        if (destTable.isPartitioned()) {
            naturalSchema.addAll(
                    destTable.getTTable().getPartitionKeys().stream()
                            .filter(f -> !staticParts.contains(f.getName()))
                            .collect(Collectors.toList()));
        }
        // we don't need to do anything if the dest schema is the same as natural schema
        if (destSchema.equals(HiveCatalog.getFieldNames(naturalSchema))) {
            return queryRelNode;
        }
        // build a list to create a Project on top of original Project
        // for each col in dest table, if it's in dest schema, store its corresponding index in the
        // dest schema, otherwise store its type and we'll create NULL for it
        List<Object> updatedIndices = new ArrayList<>(naturalSchema.size());
        for (FieldSchema col : naturalSchema) {
            int index = destSchema.indexOf(col.getName());
            if (index < 0) {
                updatedIndices.add(
                        HiveParserTypeConverter.convert(
                                TypeInfoUtils.getTypeInfoFromTypeString(col.getType()),
                                plannerContext.getTypeFactory()));
            } else {
                updatedIndices.add(index);
            }
        }
        if (queryRelNode instanceof Project) {
            return addProjectForDestSchema((Project) queryRelNode, updatedIndices);
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
            // PROJECT + SORT
            RelNode addedProject = addProjectForDestSchema((Project) sortInput, updatedIndices);
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
        } else {
            // PROJECT + DIST
            return handleDestSchemaForDist((LogicalDistribution) queryRelNode, updatedIndices);
        }
    }

    private RelNode handleDestSchemaForDist(
            LogicalDistribution hiveDist, List<Object> updatedIndices) throws SemanticException {
        Project project = (Project) hiveDist.getInput();
        RelNode addedProject = addProjectForDestSchema(project, updatedIndices);
        // disconnect the original LogicalDistribution
        hiveDist.replaceInput(0, null);
        return LogicalDistribution.create(
                addedProject,
                updateRelCollation(hiveDist.getCollation(), updatedIndices),
                updateDistKeys(hiveDist.getDistKeys(), updatedIndices));
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
        return plannerContext
                .getCluster()
                .traitSet()
                .canonize(RelCollationImpl.of(updatedCollations));
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

    private RelNode replaceProjectForStaticPart(
            Project project,
            Map<String, String> staticPartSpec,
            Table destTable,
            Map<String, RelDataType> targetColToType) {
        List<RexNode> exprs = project.getProjects();
        List<RexNode> extendedExprs = new ArrayList<>(exprs);
        int numDynmPart = destTable.getTTable().getPartitionKeys().size() - staticPartSpec.size();
        int insertIndex = extendedExprs.size() - numDynmPart;
        RexBuilder rexBuilder = plannerContext.getCluster().getRexBuilder();
        for (Map.Entry<String, String> spec : staticPartSpec.entrySet()) {
            RexNode toAdd =
                    rexBuilder.makeCharLiteral(HiveParserUtils.asUnicodeString(spec.getValue()));
            toAdd = rexBuilder.makeAbstractCast(targetColToType.get(spec.getKey()), toAdd);
            extendedExprs.add(insertIndex++, toAdd);
        }

        RelNode res =
                LogicalProject.create(
                        project.getInput(),
                        Collections.emptyList(),
                        extendedExprs,
                        (List<String>) null);
        project.replaceInput(0, null);
        return res;
    }

    private static List<String> getProjectNames(Project project) {
        return project.getNamedProjects().stream().map(p -> p.right).collect(Collectors.toList());
    }

    private RelNode addProjectForDestSchema(Project input, List<Object> updatedIndices)
            throws SemanticException {
        int destSchemaSize =
                (int) updatedIndices.stream().filter(o -> o instanceof Integer).count();
        if (destSchemaSize != input.getProjects().size()) {
            throw new SemanticException(
                    String.format(
                            "Expected %d columns, but SEL produces %d columns",
                            destSchemaSize, input.getProjects().size()));
        }
        List<RexNode> exprs = new ArrayList<>(updatedIndices.size());
        RexBuilder rexBuilder = plannerContext.getCluster().getRexBuilder();
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
