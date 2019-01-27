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
package org.apache.flink.table.plan.nodes.common

import java.lang.reflect.{Method, Modifier}
import java.util
import java.util.Collections
import com.google.common.primitives.Primitives
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.mapping.IntPair
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{RowTypeInfo, TypeExtractor}
import org.apache.flink.streaming.api.datastream.AsyncDataStream.OutputMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.streaming.api.operators.ProcessOperator
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.functions.{AsyncTableFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, InternalType, TypeConverters}
import org.apache.flink.table.api.{TableConfig, TableException, ValidationException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.TemporalJoinCodeGenerator._
import org.apache.flink.table.codegen.{CodeGeneratorContext, TemporalJoinCodeGenerator}
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow, GenericRow, JoinedRow}
import org.apache.flink.table.functions.sql.ScalarSqlFunctions
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{checkAndExtractMethods, signatureToString, signaturesToString}
import org.apache.flink.table.plan.nodes.FlinkRelNode
import org.apache.flink.table.plan.schema.{BaseRowSchema, TimeIndicatorRelDataType}
import org.apache.flink.table.plan.util.TemporalJoinUtil._
import org.apache.flink.table.plan.util.{CalcUtil, RexLiteralUtil}
import org.apache.flink.table.runtime.join.{TemporalTableJoinAsyncRunner, TemporalTableJoinProcessRunner, TemporalTableJoinWithCalcAsyncRunner, TemporalTableJoinWithCalcProcessRunner}
import org.apache.flink.table.sources.{IndexKey, LookupConfig, LookupableTableSource, TableSource}
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, TypeUtils}
import org.apache.flink.table.util.TableConnectorUtil
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Common abstract RelNode for temporal table join which shares most methods.
  * @param input  input rel node
  * @param tableSource  the table source to be temporal joined
  * @param tableRowType  the row type of the table source
  * @param tableCalcProgram  the calc (projection&filter) after table scan before joining
  * @param period   the point in time to snapshot
  */
abstract class CommonTemporalTableJoin(
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  input: RelNode,
  val tableSource: TableSource,
  tableRowType: RelDataType,
  val tableCalcProgram: Option[RexProgram],
  period: RexNode,
  val joinInfo: JoinInfo,
  val joinType: JoinRelType)
  extends SingleRel(cluster, traitSet, input) 
  with FlinkRelNode {

  val joinKeyPairs: util.List[IntPair] = getTemporalTableJoinKeyPairs(joinInfo, tableCalcProgram)
  val indexKeys: util.List[IndexKey] = getTableIndexKeys(tableSource)
  // constant keys which maybe empty if calc program is None
  val constantLookupKeys: util.Map[Int, (InternalType, Object)] = analyzeConstantLookupKeys(
    cluster, 
    tableCalcProgram, 
    indexKeys)
  val joinedIndex: Option[IndexKey] = findMatchedIndex(indexKeys, joinKeyPairs, constantLookupKeys)

  override def deriveRowType(): RelDataType = {
    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val rightType = if (tableCalcProgram.isDefined) {
      tableCalcProgram.get.getOutputRowType
    } else {
      tableRowType
    }
    SqlValidatorUtil.deriveJoinRowType(
      input.getRowType,
      rightType,
      joinType,
      flinkTypeFactory,
      null,
      Collections.emptyList[RelDataTypeField])
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val remaining = joinInfo.getRemaining(cluster.getRexBuilder)
    val joinCondition = if (remaining.isAlwaysTrue) {
      None
    } else {
      Some(remaining)
    }

    joinExplainTerms(
      super.explainTerms(pw),
      tableSource,
      input.getRowType,
      getRowType,
      tableCalcProgram,
      joinInfo.pairs(),
      joinCondition,
      joinType,
      period,
      getExpressionString)
  }

  // ----------------------------------------------------------------------------------------
  //                             Physical Translation
  // ----------------------------------------------------------------------------------------

  def translateToPlanInternal(
    inputTransformation: StreamTransformation[BaseRow],
    env: StreamExecutionEnvironment,
    config: TableConfig,
    relBuilder: RelBuilder): StreamTransformation[BaseRow] = {

    val inputSchema = new BaseRowSchema(input.getRowType)
    val tableSchema = new BaseRowSchema(tableRowType)
    val resultSchema = new BaseRowSchema(getRowType)
    val inputBaseRowType = inputSchema.internalType()
    val tableBaseRowType = tableSchema.internalType()
    val resultBaseRowType = resultSchema.internalType()
    val resultBaseRowTypeInfo = resultSchema.typeInfo()
    val tableReturnTypeInfo =
      TypeConverters.createExternalTypeInfoFromDataType(tableSource.getReturnType)
    val tableReturnClass = CommonScan.extractTableSourceTypeClass(tableSource)

    // validate whether the node is valid and supported.
    validate(
      tableSource,
      period,
      inputSchema,
      tableSchema,
      joinKeyPairs,
      constantLookupKeys,
      indexKeys,
      joinedIndex,
      joinType)

    val checkedIndexInOrder = joinedIndex.get.getDefinedColumns.map(_.intValue()).toArray
    val indexFieldTypes = checkedIndexInOrder.map(tableSchema.fieldTypes(_))
    val remainingCondition = getRemainingJoinCondition(
      cluster.getRexBuilder,
      relBuilder,
      input.getRowType,
      tableRowType,
      tableCalcProgram,
      checkedIndexInOrder,
      joinKeyPairs,
      joinInfo,
      constantLookupKeys)

    val lookupKeysFromConstant: Map[Int, RexLiteral] = constantLookupKeys.toMap.map {
      case (i, (_, o)) => (i, relBuilder.literal(o).asInstanceOf[RexLiteral])
    }

    val lookupKeyPairs = joinKeyPairs.filter(p => checkedIndexInOrder.contains(p.target))
    // lookup key index -> input field index
    val lookupKey2InputFieldIndex: Map[Int, Int] = lookupKeyPairs
                                                   .map { k => (k.target, k.source) }
                                                   .toMap
    val lookupableTableSource = tableSource.asInstanceOf[LookupableTableSource[_]]
    val lookupConfig = if (lookupableTableSource.getLookupConfig != null) {
      lookupableTableSource.getLookupConfig
    } else {
      new LookupConfig
    }
    val leftOuterJoin = joinType == JoinRelType.LEFT

    val operator = if (lookupConfig.isAsyncEnabled) {
      val asyncBufferCapacity= lookupConfig.getAsyncBufferCapacity
      val asyncTimeout = lookupConfig.getAsyncTimeoutMs
      val asyncOutputMode = lookupConfig.getAsyncOutputMode

      val asyncTableFunction = lookupableTableSource.getAsyncLookupFunction(checkedIndexInOrder)
      val parameters = Array(classOf[ResultFuture[_]]) ++
        indexFieldTypes.map(TypeUtils.getInternalClassForType(_))
      val method = getSignatureMatchedEvalMethod(
        asyncTableFunction,
        parameters)
      // eval method valid check
      if (method.isEmpty) {
        val msg = s"Given parameter types of the async lookup TableFunction of TableSource " +
          s"'${tableSource.explainSource()}' do not match the expected signature.\n" +
          s"Expected: eval${signatureToString(parameters)} \n" +
          s"Actual: eval${signaturesToString(asyncTableFunction, "eval")}"
        throw new TableException(msg)
      }
      // return type valid check
      val udtfResultType = asyncTableFunction.getResultType(Array(), Array())
      val extractedResultTypeInfo = TypeExtractor.createTypeInfo(
        asyncTableFunction,
        classOf[AsyncTableFunction[_]],
        asyncTableFunction.getClass,
        0)
      checkUdtfReturnType(
        tableSource.explainSource(),
        tableReturnTypeInfo,
        udtfResultType,
        extractedResultTypeInfo)

      val generatedFetcher = TemporalJoinCodeGenerator.generateAsyncLookupFunction(
        config,
        relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory],
        inputBaseRowType,
        resultBaseRowType,
        tableReturnTypeInfo,
        tableReturnClass,
        checkedIndexInOrder,
        lookupKey2InputFieldIndex,
        lookupKeysFromConstant,
        asyncTableFunction)

      val asyncFunc = if (tableCalcProgram.isDefined) {
        // a projection or filter after table source scan
        val calcSchema = new BaseRowSchema(tableCalcProgram.get.getOutputRowType)
        val rightTypeInfo = calcSchema.internalType
        val collector = generateAsyncCollector(
          config,
          inputBaseRowType,
          rightTypeInfo,
          remainingCondition)
        val calcMap = generateCalcMapFunction(config, tableCalcProgram, tableSchema)
        new TemporalTableJoinWithCalcAsyncRunner(
          generatedFetcher.name,
          generatedFetcher.code,
          calcMap.name,
          calcMap.code,
          collector.name,
          collector.code,
          asyncBufferCapacity,
          leftOuterJoin,
          inputSchema.fieldTypes.toArray,
          resultBaseRowTypeInfo)
      } else {
        val collector = generateAsyncCollector(
          config,
          inputBaseRowType,
          tableBaseRowType,
          remainingCondition)
        new TemporalTableJoinAsyncRunner(
          generatedFetcher.name,
          generatedFetcher.code,
          collector.name,
          collector.code,
          asyncBufferCapacity,
          leftOuterJoin,
          inputSchema.fieldTypes.toArray,
          resultBaseRowTypeInfo)
      }

      val mode = if (asyncOutputMode == LookupConfig.AsyncOutputMode.ORDERED) {
        OutputMode.ORDERED
      } else {
        OutputMode.UNORDERED
      }

      new AsyncWaitOperator(asyncFunc, asyncTimeout, asyncBufferCapacity, mode)
    } else {
      // sync join
      val lookupFunction = lookupableTableSource.getLookupFunction(checkedIndexInOrder)
      val parameters = indexFieldTypes.map(TypeUtils.getInternalClassForType(_))
      val method = getSignatureMatchedEvalMethod(
        lookupFunction,
        parameters)
      // valid check
      if (method.isEmpty) {
        val msg = s"Given parameter types of the lookup TableFunction of TableSource " +
          s"'${tableSource.explainSource()}' do not match the expected signature.\n" +
          s"Expected: eval${signatureToString(parameters)} \n" +
          s"Actual: eval${signaturesToString(lookupFunction, "eval")}"
        throw new TableException(msg)
      }
      // return type valid check
      val udtfResultType = lookupFunction.getResultType(Array(), Array())
      val extractedResultTypeInfo = TypeExtractor.createTypeInfo(
        lookupFunction,
        classOf[TableFunction[_]],
        lookupFunction.getClass,
        0)
      checkUdtfReturnType(
        tableSource.explainSource(),
        tableReturnTypeInfo,
        udtfResultType,
        extractedResultTypeInfo)

      val generatedFetcher = TemporalJoinCodeGenerator.generateLookupFunction(
        config,
        relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory],
        inputBaseRowType,
        resultBaseRowType,
        tableReturnTypeInfo,
        tableReturnClass,
        checkedIndexInOrder,
        lookupKey2InputFieldIndex,
        lookupKeysFromConstant,
        lookupFunction,
        env.getConfig.isObjectReuseEnabled)

      val ctx = CodeGeneratorContext(config)
      val processFunc = if (tableCalcProgram.isDefined) {
        // a projection or filter after table source scan
        val calcSchema = new BaseRowSchema(tableCalcProgram.get.getOutputRowType)
        val rightTypeInfo = calcSchema.internalType()
        val collector = generateCollector(
          ctx,
          config,
          inputBaseRowType,
          rightTypeInfo,
          resultBaseRowType,
          remainingCondition,
          None)
        val calcMap = generateCalcMapFunction(config, tableCalcProgram, tableSchema)
        new TemporalTableJoinWithCalcProcessRunner(
          generatedFetcher.name,
          generatedFetcher.code,
          calcMap.name,
          calcMap.code,
          collector.name,
          collector.code,
          leftOuterJoin,
          inputSchema.fieldTypes.toArray,
          resultBaseRowTypeInfo)
      } else {
        val collector = generateCollector(
          ctx,
          config,
          inputBaseRowType,
          tableBaseRowType,
          resultBaseRowType,
          remainingCondition,
          None)
        new TemporalTableJoinProcessRunner(
          generatedFetcher.name,
          generatedFetcher.code,
          collector.name,
          collector.code,
          leftOuterJoin,
          inputSchema.fieldTypes.toArray,
          resultBaseRowTypeInfo)
      }
      new ProcessOperator(processFunc)
    }

    val operatorName = joinToString(
      lookupableTableSource,
      joinType,
      resultSchema,
      inputSchema,
      tableSchema,
      remainingCondition,
      constantLookupKeys,
      joinKeyPairs,
      getExpressionString)

    new OneInputTransformation(
      inputTransformation,
      operatorName,
      operator,
      TypeConverters.toBaseRowTypeInfo(resultBaseRowType),
      inputTransformation.getParallelism)
  }

  private def rowTypeEquals(expected: TypeInformation[_], actual: TypeInformation[_]): Boolean = {
    // check internal and external type, cause we will auto convert external class to internal
    // class (eg: Row => BaseRow).
    // check both type because GenericType<Row> and GenericType<BaseRow>.
    TypeUtils.getExternalClassForType(expected) == TypeUtils.getExternalClassForType(actual) ||
        TypeUtils.getInternalClassForType(expected) == TypeUtils.getInternalClassForType(actual)
  }

  private def getSignatureMatchedEvalMethod(
    function: UserDefinedFunction,
    methodSignature: Array[Class[_]]): Option[Method] = {

    val methods = checkAndExtractMethods(function, "eval")

    var applyCnt = 0
    val filtered = methods
     // go over all the methods and filter out matching methods
     .filter {
       case cur if !cur.isVarArgs =>
         val signatures = cur.getParameterTypes
         // match parameters of signature to actual parameters
         methodSignature.length == signatures.length &&
           signatures.zipWithIndex.forall { case (clazz, i) =>
             if (methodSignature(i) == classOf[Object]) {
               // The element of the method signature comes from the Table API's
               // apply().
               // We can not decide the type here. It is an Unresolved Expression.
               // Actually, we do not have to decide the type here, any method of
               // the overrides
               // which matches the arguments count will do the job.
               // So here we choose any method is correct.
               applyCnt += 1
             }
             parameterTypeEquals(methodSignature(i), clazz)
           }
       case cur if cur.isVarArgs =>
         val signatures = cur.getParameterTypes
         methodSignature.zipWithIndex.forall {
           // non-varargs
           case (clazz, i) if i < signatures.length - 1 =>
             parameterTypeEquals(clazz, signatures(i))
           // varargs
           case (clazz, i) if i >= signatures.length - 1 =>
             parameterTypeEquals(clazz, signatures.last.getComponentType)
         } || (methodSignature.isEmpty && signatures.length == 1) // empty varargs
     }

    // if there is a fixed method, compiler will call this method preferentially
    val fixedMethodsCount = filtered.count(!_.isVarArgs)
    val found = filtered.filter { cur =>
      fixedMethodsCount > 0 && !cur.isVarArgs ||
        fixedMethodsCount == 0 && cur.isVarArgs
    }.filter { cur =>
      // filter abstract methods
      !Modifier.isVolatile(cur.getModifiers)
    }

    if (found.length > 1) {
      if (applyCnt > 0) {
        // As we can not decide type while apply() exists, so choose any one is correct
        return found.headOption
      }
      throw new ValidationException(
        s"Found multiple 'eval' methods which match the signature.")
    }
    found.headOption
  }
  
  private def parameterTypeEquals(candidate: Class[_], expected: Class[_]): Boolean = {
    candidate == null ||
      candidate == expected ||
      expected == classOf[Object] ||
      candidate == classOf[Object] || // Special case when we don't know the type
      expected.isPrimitive && Primitives.wrap(expected) == candidate ||
      (candidate.isArray &&
        expected.isArray &&
        candidate.getComponentType.isInstanceOf[Object] &&
        expected.getComponentType == classOf[Object])
  }

  private def getRemainingJoinCondition(
    rexBuilder: RexBuilder,
    relBuilder: RelBuilder,
    leftRowType: RelDataType,
    tableRowType: RelDataType,
    tableCalcProgram: Option[RexProgram],
    checkedIndexInOrder: Array[Int],
    joinKeyPairs: util.List[IntPair],
    joinInfo: JoinInfo,
    constantLookupKeys: util.Map[Int, (InternalType, Object)]): Option[RexNode] = {

    val remainingPairs = joinKeyPairs
                         .filter(p => !checkedIndexInOrder.contains(p.target))
    // convert remaining pairs to RexInputRef tuple for building sqlStdOperatorTable.EQUALS calls
    val remainingAnds = remainingPairs.map { p =>
      val leftInputRef = new RexInputRef(p.source, leftRowType.getFieldList.get(p.source).getType)
      val rightInputRef = tableCalcProgram match {
        case Some(program) =>
          val rightKeyIdx = program
                            .getOutputRowType.getFieldNames
                            .indexOf(program.getInputRowType.getFieldNames.get(p.target))
          new RexInputRef(
            leftRowType.getFieldCount + rightKeyIdx,
            program.getOutputRowType.getFieldList.get(rightKeyIdx).getType)

        case None =>
          new RexInputRef(
            leftRowType.getFieldCount + p.target,
            tableRowType.getFieldList.get(p.target).getType)
      }
      (leftInputRef, rightInputRef)
    }
    val equiAnds = relBuilder.and(remainingAnds.map(p => relBuilder.equals(p._1, p._2)): _*)
    val condition = relBuilder.and(equiAnds, joinInfo.getRemaining(rexBuilder))
    if (condition.isAlwaysTrue) {
      None
    } else {
      Some(condition)
    }
  }


  /**
    * Gets the join key pairs from left input key to temporal table key
    * @param joinInfo the join information of temporal table join
    * @param temporalTableCalcProgram the calc programs on temporal table
    */
  private def getTemporalTableJoinKeyPairs(
    joinInfo: JoinInfo,
    temporalTableCalcProgram: Option[RexProgram]): util.List[IntPair] = {
    temporalTableCalcProgram match {
      case Some(program) =>
        // the target key of joinInfo is the calc output fields, we have to remapping to table here
        val keyPairs: util.List[IntPair] = new util.ArrayList[IntPair]()
        joinInfo.pairs().map {
          p =>
            val calcSrcIdx = getIdenticalSourceField(program, p.target)
            if (calcSrcIdx != -1) {
              keyPairs.add(new IntPair(p.source, calcSrcIdx))
            }
        }
        keyPairs
      case None => joinInfo.pairs()
    }
  }

  /**
    * Analyze the constant lookup keys in the temporal table from the calc program on the temporal
    * table.
    */
  def analyzeConstantLookupKeys(
    cluster: RelOptCluster,
    temporalTableCalcProgram: Option[RexProgram],
    indexKeys: util.List[IndexKey]): util.Map[Int, (InternalType, Object)] = {

    val constantKeyMap: util.Map[Int, (InternalType, Object)] =
      new util.HashMap[Int, (InternalType, Object)]
    // all the columns in index keys
    val allKeys = mutable.HashSet.empty[Int]
    indexKeys.map(_.getDefinedColumns.map(allKeys += _))

    if (temporalTableCalcProgram.isDefined && null != temporalTableCalcProgram.get.getCondition) {
      val program = temporalTableCalcProgram.get
      val condition = RexUtil.toCnf(
        cluster.getRexBuilder,
        program.expandLocalRef(program.getCondition))
      // presume 'A = 1 AND A = 2' will be reduced to ALWAYS_FALSE
      extractConstantKeysFromEquiCondition(condition, allKeys.toArray, constantKeyMap)
    }
    constantKeyMap
  }

  private def findMatchedIndex(
    allIndexes: util.List[IndexKey],
    joinKeyPairs: util.List[IntPair],
    constantLookupKeys: util.Map[Int, (InternalType, Object)]): Option[IndexKey] = {
    val lookupKeyCandidates = joinKeyPairs.map(_.target) ++ constantLookupKeys.keySet()
    // do validation later due to unified ErrorCode
    allIndexes.find(_.isIndex(lookupKeyCandidates.toArray))
  }

  // ----------------------------------------------------------------------------------------
  //                             Physical Optimization Utilities
  // ----------------------------------------------------------------------------------------

  // this is highly inspired by Calcite's RexProgram#getSourceField(int)
  private def getIdenticalSourceField(rexProgram: RexProgram, outputOrdinal: Int): Int = {
    assert((outputOrdinal >= 0) && (outputOrdinal < rexProgram.getProjectList.size()))
    val project = rexProgram.getProjectList.get(outputOrdinal)
    var index = project.getIndex
    while (true) {
      var expr = rexProgram.getExprList.get(index)
      expr match {
        case call: RexCall if call.getOperator == SqlStdOperatorTable.IN_FENNEL =>
          // drill through identity function
          expr = call.getOperands.get(0)
        case call: RexCall if call.getOperator == SqlStdOperatorTable.CAST =>
          // drill through identity function
          expr = call.getOperands.get(0)
        case _ =>
      }
      expr match {
        case ref: RexLocalRef => index = ref.getIndex
        case ref: RexInputRef => return ref.getIndex
        case _ => return -1
      }
    }
    -1
  }

  private def extractConstantKeysFromEquiCondition(
    condition: RexNode,
    indexKeys: Array[Int],
    constantKeyMap: util.Map[Int, (InternalType, Object)]): Unit = {

    condition match {
      case c: RexCall if c.getKind == SqlKind.AND =>
        c.getOperands.foreach(r => extractConstantKeys(r, indexKeys, constantKeyMap))
      case rex: RexNode => extractConstantKeys(rex, indexKeys, constantKeyMap)
      case _ =>
    }
  }

  private def extractConstantKeys(
    pred: RexNode,
    keyIndexes: Array[Int],
    constantKeyMap: util.Map[Int, (InternalType, Object)])
  : util.Map[Int, (InternalType, Object)] = {

    pred match {
      case c: RexCall if c.getKind == SqlKind.EQUALS =>
        val leftTerm = c.getOperands.get(0)
        val rightTerm = c.getOperands.get(1)
        val t = FlinkTypeFactory.toInternalType(rightTerm.getType)
        leftTerm match {
          case rexLiteral: RexLiteral =>
            rightTerm match {
              case r: RexInputRef if keyIndexes.contains(r.getIndex) =>
                constantKeyMap.put(
                  r.getIndex,
                  (t, RexLiteralUtil.literalValue(rexLiteral)))
              case _ =>
            }
          case _ => rightTerm match {
            case rexLiteral: RexLiteral =>
              leftTerm match {
                case r: RexInputRef if keyIndexes.contains(r.getIndex) =>
                  constantKeyMap.put(
                    r.getIndex,
                    (t, RexLiteralUtil.literalValue(rexLiteral)))
                case _ =>
              }
            case _ =>
          }
        }
      case _ =>
    }
    constantKeyMap
  }

  // ----------------------------------------------------------------------------------------
  //                                       Validation
  // ----------------------------------------------------------------------------------------

  def validate(
    tableSource: TableSource,
    period: RexNode,
    inputSchema: BaseRowSchema,
    tableSourceSchema: BaseRowSchema,
    joinKeyPairs: util.List[IntPair],
    constantLookupKeys: util.Map[Int, (InternalType, Object)],
    allIndexKeys: util.List[IndexKey],
    joinedIndex: Option[IndexKey],
    joinType: JoinRelType): Unit = {

    if (joinKeyPairs.isEmpty && constantLookupKeys.isEmpty) {
      throw new TableException(
        "Temporal table join requires an equality condition on ALL of " +
          "temporal table's primary key(s) or unique key(s) or index field(s).")
    }

    // checked index never be null, so declared index also not null.
    if (allIndexKeys.isEmpty) {
      throw new TableException(
        "Temporal table require to define an primary key or unique key or index.")
    }

    // check a matched index exist
    if (joinedIndex.isEmpty) {
      throw new TableException(
        "Temporal table join requires an equality condition on ALL of " +
          "temporal table's primary key(s) or unique key(s) or index field(s).")
    }

    if (!tableSource.isInstanceOf[LookupableTableSource[_]]) {
      throw new TableException("TableSource must implement LookupableTableSource interface " +
                                 "if it is used as a temporal table.")
    }

    val checkedLookupKeys = joinedIndex.get.getDefinedColumns

    val lookupKeyPairs = joinKeyPairs.filter(p => checkedLookupKeys.contains(p.target))
    val leftKeys = lookupKeyPairs.map(_.source).toArray
    val rightKeys = lookupKeyPairs.map(_.target) ++ constantLookupKeys.keys
    val leftKeyTypes = leftKeys.map(inputSchema.fieldTypeInfos(_))
    // use original keyPair to validate key types (rigthKeys may include constant keys)
    val rightKeyTypes = lookupKeyPairs.map(p => tableSourceSchema.fieldTypeInfos(p.target))

    // check type
    leftKeyTypes.zip(rightKeyTypes).foreach(f => {
      if (f._1 != f._2) {
        val leftNames = leftKeys.map(inputSchema.fieldNames(_))
        val rightNames = rightKeys.map(tableSourceSchema.fieldNames(_))

        val leftNameTypes = leftKeyTypes
                            .zip(leftNames)
                            .map(f => s"${f._2}[${f._1.toString}]")

        val rightNameTypes = rightKeyTypes
                             .zip(rightNames)
                             .map(f => s"${f._2}[${f._1.toString}]")

        val condition = leftNameTypes
                        .zip(rightNameTypes)
                        .map(f => s"${f._1}=${f._2}")
                        .mkString(", ")
        throw new TableException("Join: Equality join predicate on incompatible types. " +
                                   s"And the condition is $condition")
      }
    })

    if (joinType != JoinRelType.LEFT && joinType != JoinRelType.INNER) {
      throw new TableException(
        "Temporal table join currently only support INNER JOIN and LEFT JOIN, " +
          "but was " + joinType.toString + " JOIN")
    }

    val tableReturnType = TypeConverters.createExternalTypeInfoFromDataType(
      tableSource.getReturnType)
    if (!tableReturnType.isInstanceOf[BaseRowTypeInfo] &&
      !tableReturnType.isInstanceOf[RowTypeInfo]) {
      throw new TableException(
        "Temporal table join only support Row or BaseRow type as return type of temporal table." +
          " But was " + tableReturnType)
    }

    // period specification check
    period.getType match {
      case t: TimeIndicatorRelDataType if !t.isEventTime => // ok
      case _ =>
        throw new TableException(
          "Currently only support join temporal table as of on left table's proctime field")
    }
    period match {
      case r: RexFieldAccess if r.getReferenceExpr.isInstanceOf[RexCorrelVariable] =>
      // it's left table's field, ok
      case call: RexCall if call.getOperator == ScalarSqlFunctions.PROCTIME =>
      // it is PROCTIME() call, ok
      case _ =>
        throw new TableException(
          "Currently only support join temporal table as of on left table's proctime field.")
    }

    // success
  }

  def checkUdtfReturnType(
    tableDesc: String,
    tableReturnTypeInfo: TypeInformation[_],
    udtfReturnType: DataType,
    extractedUdtfReturnTypeInfo: TypeInformation[_]): Unit = {
    if (udtfReturnType == null) {
      if (!rowTypeEquals(tableReturnTypeInfo, extractedUdtfReturnTypeInfo)) {
        throw new TableException(
          s"The TableSource [$tableDesc] return type $tableReturnTypeInfo " +
            s"do not match its lookup function extracted return type $extractedUdtfReturnTypeInfo")
      }
      if (extractedUdtfReturnTypeInfo.getTypeClass != classOf[BaseRow] &&
        extractedUdtfReturnTypeInfo.getTypeClass != classOf[Row]) {
        throw new TableException(
          "Result type of the async lookup TableFunction of TableSource " +
            s"'$tableDesc' is " +
            s"$extractedUdtfReturnTypeInfo type, " +
            s"currently only Row and BaseRow are supported.")
      }
    } else {
      val udtfReturnTypeInfo = TypeConverters.createExternalTypeInfoFromDataType(udtfReturnType)
      if (!rowTypeEquals(tableReturnTypeInfo, udtfReturnTypeInfo)) {
        throw new TableException(
          s"The TableSource [$tableDesc] return type $tableReturnTypeInfo " +
            s"do not match its lookup function return type $udtfReturnTypeInfo")
      }
      if (!udtfReturnTypeInfo.isInstanceOf[BaseRowTypeInfo] &&
        !udtfReturnTypeInfo.isInstanceOf[RowTypeInfo]) {
        throw new TableException(
          "Result type of the async lookup TableFunction of TableSource " +
            s"'$tableDesc' is $udtfReturnTypeInfo type, " +
            s"currently only Row and BaseRow are supported.")
      }
    }
  }

  // ----------------------------------------------------------------------------------------
  //                              toString Utilities
  // ----------------------------------------------------------------------------------------

  private def joinSelectionToString(inputType: RelDataType): String = {
    inputType.getFieldNames.toList.mkString(", ")
  }

  private def joinConditionToString(
    inputType: RelDataType,
    joinCondition: RexNode,
    expression: (RexNode, List[String], Option[List[RexNode]]) => String): String = {

    val inFields = inputType.getFieldNames.toList
    if (joinCondition != null) {
      expression(joinCondition, inFields, None)
    } else {
      null
    }
  }

  private def joinTypeToString(joinType: JoinRelType): String = joinType match {
    case JoinRelType.INNER => "InnerJoin"
    case JoinRelType.LEFT => "LeftOuterJoin"
    case JoinRelType.RIGHT => "RightOuterJoin"
    case JoinRelType.FULL => "FullOuterJoin"
  }

  private def joinToString(
    tableSource: LookupableTableSource[_],
    joinType: JoinRelType,
    joinResultSchema: BaseRowSchema,
    inputSchema: BaseRowSchema,
    tableSchema: BaseRowSchema,
    joinCondition: Option[RexNode],
    constantLookupKeys: util.Map[Int, (InternalType, Object)],
    lookupKeyPairs: util.List[IntPair],
    expression: (RexNode, List[String], Option[List[RexNode]]) => String): String = {

    val isAsyncEnabled = if (tableSource.getLookupConfig != null) {
      tableSource.getLookupConfig.isAsyncEnabled
    } else {
      (new LookupConfig).isAsyncEnabled
    }
    val prefix = if (isAsyncEnabled) {
      "AsyncJoinTable"
    } else {
      "JoinTable"
    }
    var str = s"$prefix(table: (${tableSource.explainSource()})" +
      s", joinType: ${joinTypeToString(joinType)}" +
      s", join: (${joinSelectionToString(inputSchema.relDataType)}), "

    val inputFieldNames = inputSchema.fieldNames
    val tableFieldNames = tableSchema.fieldNames
    val keyPairNames = lookupKeyPairs.map { p =>
      s"${inputFieldNames(p.source)}=${
        if (p.target > -1) tableFieldNames(p.target) else -1
      }"
    }
    str += s" on: (${keyPairNames.mkString(", ")}"
    str +=
      s"${constantLookupKeys.map(k => tableFieldNames(k._1) + " = " + k._2)
          .mkString(", ")})"
    if (joinCondition.isDefined) {
      val joinConditionString = joinConditionToString(
        joinResultSchema.relDataType,
        joinCondition.get,
        expression)
      str += s", where: ($joinConditionString)"
    }
    str += ")"
    str
  }

  def joinExplainTerms(
    pw: RelWriter,
    tableSource: TableSource,
    inputType: RelDataType,
    joinResultType: RelDataType,
    calcProgram: Option[RexProgram],
    lookupKeyPairs: util.List[IntPair],
    joinCondition: Option[RexNode],
    joinType: JoinRelType,
    period: RexNode,
    expression: (RexNode, List[String], Option[List[RexNode]]) => String): RelWriter = {

    val condition: String = if (calcProgram.isDefined) {
      CalcUtil.conditionToString(calcProgram.get, expression)
    } else {
      ""
    }
    var source = tableSource.explainSource()
    if (source == null || source.isEmpty) {
      source = TableConnectorUtil.generateRuntimeName(
        tableSource.getClass, tableSource.getTableSchema.getColumnNames)
    }

    val inputFieldNames = inputType.getFieldNames
    val tableFieldNames = tableSource.getTableSchema.getColumnNames
    val keyPairNames = lookupKeyPairs.map { p =>
      s"${inputFieldNames(p.source)}=${
        if (p.target >= 0 && p.target < tableFieldNames.length) tableFieldNames(p.target) else -1
      }"
    }

    pw.item("join", joinSelectionToString(joinResultType))
    .item("source", source)
    .item("on", keyPairNames.mkString(", "))
    .item("joinType", joinTypeToString(joinType))
    .itemIf("where", condition, !condition.isEmpty)
    .itemIf("joinCondition",
            joinConditionToString(joinResultType, joinCondition.orNull, expression),
            joinCondition.isDefined)
    .item("period", period)
  }
}
