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

package org.apache.flink.table.codegen

import java.lang.{Long => JLong}
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex._
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.functions.Function
import org.apache.flink.cep.pattern.conditions.{IterativeCondition, RichIterativeCondition}
import org.apache.flink.cep._
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableConfig, TableConfigOptions, TableException}
import org.apache.flink.table.api.types._
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.codegen.MatchCodeGenerator._
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.plan.util.AggregateUtil
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.functions.sql.ProctimeSqlFunction
import org.apache.flink.table.plan.util.MatchUtil.AggregationPatternVariableFinder
import org.apache.flink.table.runtime.conversion.DataStructureConverters.genToInternal
import org.apache.flink.table.runtime.functions.{AggsHandleFunction, ExecutionContextImpl}
import org.apache.flink.table.typeutils.TypeUtils
import org.apache.flink.table.utils.EncodingUtils
import org.apache.flink.util.Collector
import org.apache.flink.util.MathUtils.checkedDownCast

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable

/**
  * A code generator for generating CEP related functions.
  *
  * @param ctx the cotext of the code generator
  * @param nullableInput input(s) can be null.
  * @param nullCheck whether to do null check
  * @param patternNames sorted sequence of pattern variables
  * @param currentPattern if generating condition the name of pattern, which the condition will
  *                       be applied to
  */
class MatchCodeGenerator(
    ctx: CodeGeneratorContext,
    relBuilder: RelBuilder,
    nullableInput: Boolean,
    nullCheck: Boolean,
    patternNames: Seq[String],
    currentPattern: Option[String] = None,
    collectorTerm: String = CodeGeneratorContext.DEFAULT_COLLECTOR_TERM)
  extends ExprCodeGenerator(ctx, nullableInput, nullCheck) {

  private case class GeneratedPatternList(resultTerm: String, code: String, filled: Boolean = true)

  private case class GeneratedClassifierList(
      resultTerm: String, code: String, filled: Boolean = true)

  /**
    * Used to assign unique names for list of events per pattern variable name. Those lists
    * are treated as inputs and are needed by input access code.
    */
  private val reusablePatternLists: mutable.HashMap[(Boolean, String), GeneratedPatternList] =
    mutable.HashMap[(Boolean, String), GeneratedPatternList]()

  private val reusableClassiferList: mutable.HashMap[Boolean, GeneratedClassifierList] =
    mutable.HashMap[Boolean, GeneratedClassifierList]()

  private val reusableInputUnboxingExprs: mutable.Map[(String, Int), GeneratedExpression] =
    mutable.Map[(String, Int), GeneratedExpression]()

  private val reusablePerRecordStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  /**
    * Used to deduplicate aggregations calculation. The deduplication is performed by
    * [[RexNode#toString]]. Those expressions needs to be accessible from splits, if such exists.
    */
  private val reusableAggregationExpr = new mutable.HashMap[String, GeneratedExpression]()

  /**
    * Context information used by Pattern reference variable to index rows mapped to it.
    * Indexes element at offset either from beginning or the end based on the value of first.
    */
  private var offset: Int = 0
  private var first : Boolean = false

  private var oneRowPerMatch: Boolean = false
  private var running: Boolean = false

  /**
    * Flags that tells if we generate expressions inside an aggregate. It tells how to access input
    * row.
    */
  private var isWithinAggExprState: Boolean = false

  /**
    * Used to collect all aggregates per pattern variable.
    */
  private val aggregatesPerVariable = new mutable.HashMap[(Boolean, String), AggBuilder]

  /**
    * Name of term in function used to transform input row into aggregate input row.
    */
  private val inputAggRowTerm = "inAgg"

  private val keyRowTerm = "keyRow"

  /**
    * @return term of pattern names
    */
  private val patternNamesTerm = newName("patternNames")

  private lazy val eventTypeTerm = boxedTypeTermForType(input1Type)

  /**
    * Sets the new reference variable indexing context. This should be used when resolving logical
    * offsets = LAST/FIRST
    *
    * @param first  true if indexing from the beginning, false otherwise
    * @param offset offset from either beginning or the end
    */
  private def updateOffsets(first: Boolean, offset: Int): Unit = {
    this.first = first
    this.offset = offset
  }

  /** Resets indexing context of Pattern variable. */
  private def resetOffsets(): Unit = {
    first = false
    offset = 0
  }

  private def setRunning(running: Boolean): Unit = {
    this.running = running
  }

  private def resetRunning(): Unit = {
    this.running = false
  }

  private def setOneRowPerMatch(oneRowPerMatch: Boolean): Unit = {
    this.oneRowPerMatch = oneRowPerMatch
  }

  private def reusePatternLists(): String = {
    reusablePatternLists.values.map(_.code).mkString("\n")
  }

  private def reuseClassifierLists(): String = {
    reusableClassiferList.values.map(_.code).mkString("\n")
  }

  private def reuseInputUnboxingCode(): String = {
    reusableInputUnboxingExprs.values.map(_.code).mkString("\n")
  }

  private def reusePerRecordCode(): String = {
    reusablePerRecordStatements.mkString("\n")
  }

  private def addReusablePatternNames(): Unit = {
    ctx.addReusableMember(s"private String[] $patternNamesTerm = new String[] { ${
        patternNames.map(p => s""""${EncodingUtils.escapeJava(p)}"""").mkString(", ")
      } };")
  }

  /**
    * Generates a [[org.apache.flink.api.common.functions.Function]] that can be passed to Java
    * compiler.
    *
    * @param name Class name of the Function. Must not be unique but has to be a valid Java class
    *             identifier.
    * @param clazz Flink Function to be generated.
    * @param bodyCode code contents of the SAM (Single Abstract Method). Inputs, collector, or
    *                 output record can be accessed via the given term methods.
    * @tparam F Flink Function to be generated.
    * @tparam T Return type of the Flink Function.
    * @return instance of GeneratedFunction
    */
  def generateMatchFunction[F <: Function, T <: Any](
      name: String,
      config: TableConfig,
      clazz: Class[F],
      bodyCode: String)
  : GeneratedClass[_] = {
    val funcName = newName(name)
    val collectorTypeTerm = classOf[Collector[Any]].getCanonicalName
    val inputTypeTerm = boxedTypeTermForType(input1Type)
    val (functionClass, signature, inputStatements, unboxingCodeSplit) =
      if (clazz == classOf[RichIterativeCondition[_]]) {
        val baseClass = classOf[RichIterativeCondition[_]]
        val contextType = classOf[IterativeCondition.Context[_]].getCanonicalName
        val unboxingCodeSplit = generateSplitFunctionCalls(
          ctx.reusableInputUnboxingExprs.values.map(_.code).toSeq,
          config.getConf.getInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX),
          "inputUnbox",
          "private final void",
          ctx.reuseFieldCode().length,
          defineParams = s"$inputTypeTerm $input1Term, " +
            s"${classOf[IterativeCondition.Context[_]].getCanonicalName} $contextTerm",
          callingParams = s"$input1Term, $contextTerm"
        )

        (baseClass,
          s"boolean filter(Object _in1, $contextType $contextTerm)",
          List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;"),
          unboxingCodeSplit)
      } else if (clazz == classOf[RichPatternSelectFunction[_, _]]) {
        val baseClass = classOf[RichPatternSelectFunction[_, _]]
        val unboxingCodeSplit = generateSplitFunctionCalls(
          ctx.reusableInputUnboxingExprs.values.map(_.code).toSeq,
          config.getConf.getInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX),
          "inputUnbox",
          "private final void",
          ctx.reuseFieldCode().length,
          defineParams = s"java.util.Map<String, java.util.List<$inputTypeTerm>> $input1Term",
          callingParams = input1Term
        )

        (baseClass,
          s"Object select(java.util.Map<String, java.util.List<$inputTypeTerm>> $input1Term)",
          List(),
          unboxingCodeSplit)
      } else if (clazz == classOf[RichPatternFlatSelectFunction[_, _]]) {
        val baseClass = classOf[RichPatternFlatSelectFunction[_, _]]
        val unboxingCodeSplit = generateSplitFunctionCalls(
          ctx.reusableInputUnboxingExprs.values.map(_.code).toSeq,
          config.getConf.getInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX),
          "inputUnbox",
          "private final void",
          ctx.reuseFieldCode().length,
          defineParams = s"java.util.Map<String, java.util.List<$inputTypeTerm>> $input1Term, " +
            s"$collectorTypeTerm $collectorTerm",
          callingParams = s"$input1Term, $collectorTerm"
        )

        (baseClass,
          s"void flatSelect(java.util.Map<String, java.util.List<$inputTypeTerm>> $input1Term, " +
            s"$collectorTypeTerm $collectorTerm)",
          List(),
          unboxingCodeSplit)
      } else if (clazz == classOf[RichPatternTimeoutFunction[_, _]]) {
        val baseClass = classOf[RichPatternTimeoutFunction[_, _]]
        val unboxingCodeSplit = generateSplitFunctionCalls(
          ctx.reusableInputUnboxingExprs.values.map(_.code).toSeq,
          config.getConf.getInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX),
          "inputUnbox",
          "private final void",
          ctx.reuseFieldCode().length,
          defineParams = s"java.util.Map<String, java.util.List<$inputTypeTerm>> $input1Term, " +
            s"long timeoutTimestamp",
          callingParams = s"$input1Term, timeoutTimestamp"
        )

        (baseClass,
          s"Object timeout(java.util.Map<String, java.util.List<Object>> $input1Term, " +
            "long timeoutTimestamp)",
          List(),
          unboxingCodeSplit)
      } else if (clazz == classOf[RichPatternFlatTimeoutFunction[_, _]]) {
        val baseClass = classOf[RichPatternFlatTimeoutFunction[_, _]]
        val unboxingCodeSplit = generateSplitFunctionCalls(
          ctx.reusableInputUnboxingExprs.values.map(_.code).toSeq,
          config.getConf.getInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX),
          "inputUnbox",
          "private final void",
          ctx.reuseFieldCode().length,
          defineParams = s"java.util.Map<String, java.util.List<$inputTypeTerm>> $input1Term, " +
            s"$collectorTypeTerm $collectorTerm",
          callingParams = s"$input1Term, $collectorTerm"
        )

        (baseClass,
          s"void timeout(java.util.Map<String, java.util.List<Object>> $input1Term, " +
            s"long timeoutTimestamp, $collectorTypeTerm $collectorTerm)",
          List(),
          unboxingCodeSplit)
      } else {
        throw new CodeGenException("Unsupported Function.")
      }

    val funcCode = if (unboxingCodeSplit.isSplit) {
      j"""
        public class $funcName extends ${functionClass.getCanonicalName} {
          ${ctx.reuseMemberCode()}
          ${ctx.reuseFieldCode()}

          public $funcName(Object[] references) throws Exception {
            ${ctx.reuseInitCode()}
          }

          @Override
          public void open(${classOf[Configuration].getCanonicalName} parameters) throws Exception {
            ${ctx.reuseOpenCode()}
          }

          @Override
          public $signature throws Exception {
            ${inputStatements.mkString("\n")}
            ${reusePatternLists()}
            ${reuseClassifierLists()}
            ${unboxingCodeSplit.callings.mkString("\n")}
            ${ctx.reusePerRecordCode()}
            $bodyCode
          }

          ${
            unboxingCodeSplit.definitions.zip(unboxingCodeSplit.bodies) map {
              case (define, body) =>
                s"""
                   |$define throws Exception {
                   | ${ctx.reusePerRecordCode()}
                   | $body
                   |}
                   """.stripMargin
            } mkString "\n"
          }

          @Override
          public void close() throws Exception {
            ${ctx.reuseCloseCode()}
          }
        }
      """.stripMargin
    } else {
      j"""
        public class $funcName extends ${functionClass.getCanonicalName} {
          ${ctx.reuseMemberCode()}

          public $funcName(Object[] references) throws Exception {
            ${ctx.reuseInitCode()}
          }

          @Override
          public void open(${classOf[Configuration].getCanonicalName} parameters) throws Exception {
            ${ctx.reuseOpenCode()}
          }

          @Override
          public $signature throws Exception {
            ${inputStatements.mkString("\n")}
            ${reusePatternLists()}
            ${reuseClassifierLists()}
            ${ctx.reusePerRecordCode()}
            ${ctx.reuseFieldCode()}
            ${ctx.reuseInputUnboxingCode()}
            $bodyCode
          }

          @Override
          public void close() throws Exception {
            ${ctx.reuseCloseCode()}
          }
        }
      """.stripMargin
    }

    if (clazz == classOf[RichIterativeCondition[_]]) {
      GeneratedIterativeCondition(funcName, funcCode, ctx.references.toArray)
    } else if (clazz == classOf[RichPatternSelectFunction[_, _]]) {
      GeneratedPatternSelectFunction(funcName, funcCode, ctx.references.toArray)
    } else if (clazz == classOf[RichPatternFlatSelectFunction[_, _]]) {
      GeneratedPatternFlatSelectFunction(funcName, funcCode, ctx.references.toArray)
    } else if (clazz == classOf[RichPatternTimeoutFunction[_, _]]) {
      GeneratedPatternTimeoutFunction(funcName, funcCode, ctx.references.toArray)
    } else if (clazz == classOf[RichPatternFlatTimeoutFunction[_, _]]) {
      GeneratedPatternFlatTimeoutFunction(funcName, funcCode, ctx.references.toArray)
    } else {
      throw new CodeGenException("Unsupported Function.")
    }
  }

  def generateOneRowPerMatchExpression(
      partitionKeys: java.util.List[RexNode],
      measures: java.util.Map[String, RexNode],
      returnSchema: BaseRowSchema): GeneratedExpression = {
    setOneRowPerMatch(oneRowPerMatch = true)

    // For "ONE ROW PER MATCH", the output columns include:
    // 1) the partition columns;
    // 2) the columns defined in the measures clause.
    val resultExprs =
      partitionKeys.asScala.map { case inputRef: RexInputRef =>
        generatePartitionKeyAccess(inputRef)
      } ++ returnSchema.fieldNames.filter(measures.containsKey(_)).map { fieldName =>
        generateExpression(measures.get(fieldName))
      }

    val resultCodeGenerator = new ExprCodeGenerator(ctx, nullableInput, nullCheck)
        .bindInput(input1Type, inputTerm = input1Term)
    val resultExpression = resultCodeGenerator.generateResultExpression(
      resultExprs,
      new RowType(
        returnSchema.fieldTypeInfos,
        returnSchema.fieldNames.toArray),
      classOf[GenericRow])

    aggregatesPerVariable.values.foreach(_.generateAggFunction())

    resultExpression
  }

  def generateAllRowsPerMatchExpression(
      partitionKeys: java.util.List[RexNode],
      orderKeys: RelCollation,
      measures: java.util.Map[String, RexNode],
      returnSchema: BaseRowSchema): GeneratedExpression = {

    val patternNameTerm = newName("patternName")
    val eventNameTerm = newName("event")
    val eventNameListTerm = newName("eventList")
    val listTypeTerm = classOf[java.util.List[_]].getCanonicalName

    setOneRowPerMatch(oneRowPerMatch = false)

    // For "ALL ROWS PER MATCH", the output columns include:
    // 1) the partition columns;
    // 2) the ordering columns;
    // 3) the columns defined in the measures clause;
    // 4) any remaining columns defined of the input.
    val fieldsAccessed = mutable.Set[Int]()
    val resultExprs =
      partitionKeys.asScala.map { case inputRef: RexInputRef =>
        fieldsAccessed += inputRef.getIndex
        generateFieldAccess(ctx, input1Type, eventNameTerm, inputRef.getIndex, nullCheck)
      } ++ orderKeys.getFieldCollations.asScala.map { fieldCollation =>
        fieldsAccessed += fieldCollation.getFieldIndex
        generateFieldAccess(ctx, input1Type, eventNameTerm, fieldCollation.getFieldIndex, nullCheck)
      } ++ (0 until TypeUtils.getArity(input1Type)).filterNot(fieldsAccessed.contains).map { idx =>
        generateFieldAccess(ctx, input1Type, eventNameTerm, idx, nullCheck)
      } ++ returnSchema.fieldNames.filter(measures.containsKey(_)).map { fieldName =>
        generateExpression(measures.get(fieldName))
      }

    val resultCodeGenerator = new ExprCodeGenerator(ctx, nullableInput, nullCheck)
        .bindInput(input1Type, inputTerm = input1Term)
    val resultExpression = resultCodeGenerator.generateResultExpression(
      resultExprs,
      new RowType(
        returnSchema.fieldTypeInfos,
        returnSchema.fieldNames.toArray),
      classOf[GenericRow])

    val resultCode = {
      addReusablePatternNames()

      def fillPatternLists(): String = {
        reusablePatternLists.filterNot(_._2.filled).map { patternList =>
          val patternName = patternList._1._2
          val listName = patternList._2.resultTerm
          if (patternName == ALL_PATTERN_VARIABLE) {
            j"""
               |$listName.add($eventNameTerm);
               |""".stripMargin
          } else {
            val escapedPatternName = EncodingUtils.escapeJava(patternName)

            j"""
               |if ($patternNameTerm.equals("$escapedPatternName")) {
               |  $listName.add($eventNameTerm);
               |}
               |""".stripMargin
          }
        }.mkString("\n")
      }

      def fillClassifierLists(): String = {
        reusableClassiferList.filterNot(_._2.filled).map { classiferList =>
          val listName = classiferList._2.resultTerm
          j"""
             |$listName.add($patternNameTerm);
             |""".stripMargin
        }.mkString("\n")
      }

      j"""
         |for (String $patternNameTerm : $patternNamesTerm) {
         |  $listTypeTerm $eventNameListTerm = ($listTypeTerm) $input1Term.get($patternNameTerm);
         |  if ($eventNameListTerm != null) {
         |    for ($eventTypeTerm $eventNameTerm : $eventNameListTerm) {
         |      ${fillPatternLists()}
         |      ${fillClassifierLists()}
         |      ${reuseInputUnboxingCode()}
         |      ${reusePerRecordCode()}
         |      ${resultExpression.code}
         |      $collectorTerm.collect(${resultExpression.resultTerm});
         |    }
         |  }
         |}
         |""".stripMargin
    }

    aggregatesPerVariable.values.foreach(_.generateAggFunction())

    GeneratedExpression("", "false", resultCode, null)
  }

  def generateCondition(call: RexNode): GeneratedExpression = {
    val exp = call.accept(this)
    aggregatesPerVariable.values.foreach(_.generateAggFunction())
    exp
  }

  override def visitCall(call: RexCall): GeneratedExpression = {
    call.getOperator match {
      case PREV | NEXT =>
        val countLiteral = call.getOperands.get(1).asInstanceOf[RexLiteral]
        val count = checkedDownCast(countLiteral.getValueAs(classOf[JLong]))
        if (count != 0) {
          throw new TableException("Flink does not support physical offsets within partition.")
        } else {
          updateOffsets(first = false, 0)
          val exp = call.getOperands.get(0).accept(this)
          resetOffsets()
          exp
        }

      case FIRST | LAST =>
        val countLiteral = call.getOperands.get(1).asInstanceOf[RexLiteral]
        val offset = checkedDownCast(countLiteral.getValueAs(classOf[JLong]))
        updateOffsets(call.getOperator == FIRST, offset)
        val expr = call.operands.get(0).accept(this)
        resetOffsets()
        expr

      case CLASSIFIER => findClassifierByLogicalPosition()

      case RUNNING =>
        if (oneRowPerMatch) {
          // running is the same as final
          call.getOperands.get(0).accept(this)
        } else {
          setRunning(true)
          val expr = call.operands.get(0).accept(this)
          resetRunning()
          expr
        }

      case FINAL => call.getOperands.get(0).accept(this)

      case _: SqlAggFunction =>
        val variable = call.accept(new AggregationPatternVariableFinder)
          .getOrElse(throw new TableException("No pattern variable specified in aggregate"))

        val matchAgg = aggregatesPerVariable.get((running, variable)) match {
          case Some(agg) => agg
          case None =>
            val agg = new AggBuilder(variable)
            aggregatesPerVariable((running, variable)) = agg
            agg
        }

        matchAgg.generateDeduplicatedAggAccess(call)

      case ProctimeSqlFunction =>
          MatchCodeGenerator.generateProctimeTimestamp()

      case _ => super.visitCall(call)
    }
  }

  /**
    * Extracts partition keys from any element of the match
    *
    * @param partitionKey partition key to be extracted
    * @return generated code for the given key
    */
  private def generatePartitionKeyAccess(partitionKey: RexInputRef): GeneratedExpression = {
    val keyRow = generateKeyRow()
    generateFieldAccess(ctx, keyRow.resultType, keyRow.resultTerm, partitionKey.getIndex, nullCheck)
  }

  private def generateKeyRow(): GeneratedExpression = {
    val exp = ctx.getReusableInputUnboxingExprs(keyRowTerm, 0) match {
      case Some(expr) =>
        expr

      case None =>
        val nullTerm = newName("isNull")

        ctx.addReusableMember(s"$eventTypeTerm $keyRowTerm;")

        val keyCode =
          j"""
             |boolean $nullTerm = true;
             |for (java.util.Map.Entry entry : $input1Term.entrySet()) {
             |  java.util.List value = (java.util.List) entry.getValue();
             |  if (value != null && value.size() > 0) {
             |    $keyRowTerm = ($eventTypeTerm) value.get(0);
             |    $nullTerm = false;
             |    break;
             |  }
             |}
             |""".stripMargin

        val exp = GeneratedExpression(keyRowTerm, nullTerm, keyCode, input1Type)
        ctx.addReusableInputUnboxingExprs(keyRowTerm, 0, exp)
        exp
    }
    exp.copy(code = NO_CODE)
  }

  override def visitPatternFieldRef(fieldRef: RexPatternFieldRef): GeneratedExpression = {
    if (isWithinAggExprState) {
      generateFieldAccess(ctx, input1Type, inputAggRowTerm, fieldRef.getIndex, nullCheck)
    } else {
      if (fieldRef.getAlpha.equals(ALL_PATTERN_VARIABLE) &&
        currentPattern.isDefined && offset == 0 && !first) {
        generateInputAccess(
          ctx, input1Type, input1Term, fieldRef.getIndex, nullableInput, nullCheck)
      } else {
        generatePatternFieldRef(fieldRef)
      }
    }
  }

  private def generateDefinePatternVariableExp(
      patternName: String,
      currentPattern: String)
  : GeneratedPatternList = {
    val Seq(listName, eventNameTerm) = newNames(Seq("patternEvents", "event"))

    ctx.addReusableMember(s"java.util.List $listName;")

    val addCurrent = if (currentPattern == patternName || patternName == ALL_PATTERN_VARIABLE) {
      j"""
         |$listName.add($input1Term);
         |""".stripMargin
    } else {
      ""
    }

    val listCode = if (patternName == ALL_PATTERN_VARIABLE) {
      addReusablePatternNames()
      val patternTerm = newName("pattern")

      j"""
         |$listName = new java.util.ArrayList();
         |for (String $patternTerm : $patternNamesTerm) {
         |  for ($eventTypeTerm $eventNameTerm :
         |  $contextTerm.getEventsForPattern($patternTerm)) {
         |    $listName.add($eventNameTerm);
         |  }
         |}
         |""".stripMargin
    } else {
      val escapedPatternName = EncodingUtils.escapeJava(patternName)
      j"""
         |$listName = new java.util.ArrayList();
         |for ($eventTypeTerm $eventNameTerm :
         |  $contextTerm.getEventsForPattern("$escapedPatternName")) {
         |    $listName.add($eventNameTerm);
         |}
         |""".stripMargin
    }

    val code =
      j"""
         |$listCode
         |$addCurrent
         |""".stripMargin

    GeneratedPatternList(listName, code)
  }

  private def generateMeasurePatternVariableExp(patternName: String): GeneratedPatternList = {
    val Seq(listName, patternTerm) = newNames(Seq("patternEvents", "pattern"))

    ctx.addReusableMember(s"java.util.List $listName;")

    val (code, filled) = if (running) {
      val code =
        j"""
           |$listName = new java.util.ArrayList();
           |""".stripMargin
      (code, false)
    } else if (patternName == ALL_PATTERN_VARIABLE) {
      addReusablePatternNames()

      val code =
        j"""
           |$listName = new java.util.ArrayList();
           |for (String $patternTerm : $patternNamesTerm) {
           |  java.util.List rows = (java.util.List) $input1Term.get($patternTerm);
           |  if (rows != null) {
           |    $listName.addAll(rows);
           |  }
           |}
           |""".stripMargin
      (code, true)
    } else {
      val escapedPatternName = EncodingUtils.escapeJava(patternName)

      val code =
        j"""
           |$listName = (java.util.List) $input1Term.get("$escapedPatternName");
           |if ($listName == null) {
           |  $listName = java.util.Collections.emptyList();
           |}
           |""".stripMargin
      (code, true)
    }

    GeneratedPatternList(listName, code, filled)
  }

  private def findEventByLogicalPosition(
      patternFieldAlpha: String)
  : GeneratedExpression = {
    val rowNameTerm = newName("row")

    val listName = findEventsByPatternName(patternFieldAlpha).resultTerm
    val resultIndex = if (first) {
      j"""$offset"""
    } else {
      j"""$listName.size() - $offset - 1"""
    }

    ctx.addReusableMember(s"$eventTypeTerm $rowNameTerm;")

    val funcCode =
      j"""
         |$rowNameTerm = null;
         |if ($listName.size() > $offset) {
         |  $rowNameTerm = (($eventTypeTerm) $listName.get($resultIndex));
         |}
         |""".stripMargin

    GeneratedExpression(rowNameTerm, "", funcCode, input1Type)
  }

  private def findEventsByPatternName(
      patternFieldAlpha: String): GeneratedPatternList = {
    reusablePatternLists.get((running, patternFieldAlpha)) match {
      case Some(expr) =>
        expr

      case None =>
        val exp = currentPattern match {
          case Some(p) => generateDefinePatternVariableExp(patternFieldAlpha, p)
          case None => generateMeasurePatternVariableExp(patternFieldAlpha)
        }
        reusablePatternLists((running, patternFieldAlpha)) = exp
        exp
    }
  }

  private def generateDefineClassifierVariableExp(currentPattern: String)
      : GeneratedClassifierList = {
    val Seq(listName, eventNameTerm) = newNames(Seq("classifiers", "event"))

    ctx.addReusableMember(s"java.util.List $listName;")

    val addCurrent =
      j"""
         |$listName.add("$currentPattern");
         |""".stripMargin

    val listCode = {
      val patternNamesToVisit = patternNames.take(patternNames.indexOf(currentPattern) + 1)

      for (patternName <- patternNamesToVisit) yield {
        val escapedPatternName = EncodingUtils.escapeJava(patternName)
        j"""
           |for ($eventTypeTerm $eventNameTerm :
           |  $contextTerm.getEventsForPattern("$escapedPatternName")) {
           |    $listName.add("${EncodingUtils.escapeJava(patternName)}");
           |}
           |""".stripMargin
      }
    }.mkString("\n")

    val code =
      j"""
         |$listName = new java.util.ArrayList();
         |$listCode
         |$addCurrent
         |""".stripMargin

    GeneratedClassifierList(listName, code)
  }

  private def generateMeasureClassifierVariableExp(): GeneratedClassifierList = {
    val Seq(listName, patternTerm, eventNameTerm) = newNames(Seq("classifiers", "pattern", "event"))

    ctx.addReusableMember(s"java.util.List $listName;")

    val (code, filled) = if (!oneRowPerMatch && running) {
      val code =
        j"""
           |$listName = new java.util.ArrayList();
           |""".stripMargin
      (code, false)
    } else {
      addReusablePatternNames()

      val code =
        j"""
           |$listName = new java.util.ArrayList();
           |for (String $patternTerm : $patternNamesTerm) {
           |  java.util.List rows = (java.util.List)
           |    $input1Term.get(${EncodingUtils.escapeJava(patternTerm)});
           |  if (rows != null) {
           |    for ($eventTypeTerm $eventNameTerm : rows) {
           |      $listName.add($patternTerm);
           |    }
           |  }
           |}
           |""".stripMargin
      (code, true)
    }

    GeneratedClassifierList(listName, code, filled)
  }

  private def findClassifierByLogicalPosition(): GeneratedExpression = {
    val Seq(nullTerm, classifierTerm) = newNames(Seq("isNull", "classifier"))

    val listName = findClassifiers().resultTerm
    val resultIndex = if (first) {
      j"""$offset"""
    } else {
      j"""$listName.size() - $offset - 1"""
    }

    val resultType = StringType.INSTANCE
    val resultTypeTerm = primitiveTypeTermForType(resultType)
    val resultInternal = genToInternal(ctx, resultType, s"$listName.get($resultIndex)")

    ctx.addReusableMember(s"$resultTypeTerm $classifierTerm;")

    val funcCode =
      j"""
         |boolean $nullTerm = true;
         |$classifierTerm = null;
         |if ($listName.size() > $offset) {
         |  $classifierTerm = ($resultTypeTerm) $resultInternal;
         |  $nullTerm = false;
         |}
         |""".stripMargin

    GeneratedExpression(classifierTerm, nullTerm, funcCode, resultType)
  }

  private def findClassifiers(): GeneratedClassifierList = {
    reusableClassiferList.get(running) match {
      case Some(expr) =>
        expr

      case None =>
        val exp = currentPattern match {
          case Some(p) => generateDefineClassifierVariableExp(p)
          case None => generateMeasureClassifierVariableExp()
        }
        reusableClassiferList(running) = exp
        exp
    }
  }

  private def generatePatternFieldRef(fieldRef: RexPatternFieldRef): GeneratedExpression = {
    val escapedAlpha = EncodingUtils.escapeJava(fieldRef.getAlpha)

    val patternVariableRef = getReusableInputUnboxingExprs(s"$escapedAlpha#$first", offset) match {
      case Some(expr) =>
        expr

      case None =>
        val exp = findEventByLogicalPosition(fieldRef.getAlpha)
        addReusableInputUnboxingExprs(s"$escapedAlpha#$first", offset, exp)
        exp
    }

    generateNullableInputFieldAccess(
      ctx,
      patternVariableRef.resultType,
      patternVariableRef.resultTerm,
      fieldRef.getIndex,
      nullCheck)
  }

  private def addReusablePerRecordStatement(s: String): Unit = {
    if (running) {
      reusablePerRecordStatements.add(s)
    } else {
      ctx.addPerRecordStatement(s)
    }
  }

  private def getReusableInputUnboxingExprs(inputTerm: String, index: Int)
      : Option[GeneratedExpression] =
    if (running) {
      reusableInputUnboxingExprs.get((inputTerm, index))
    } else {
      ctx.getReusableInputUnboxingExprs(inputTerm, index)
    }

  private def addReusableInputUnboxingExprs(
      inputTerm: String, index: Int, expr: GeneratedExpression): Unit =
    if (running) {
      reusableInputUnboxingExprs((inputTerm, index)) = expr
    } else {
      ctx.addReusableInputUnboxingExprs(inputTerm, index, expr)
    }

  class AggBuilder(variable: String) {

    private val aggregates = new mutable.ListBuffer[RexCall]()

    private val variableUID = newName("variable")

    private val calculateAggFuncName = s"calculateAgg_$variableUID"

    def generateDeduplicatedAggAccess(aggCall: RexCall): GeneratedExpression = {
      reusableAggregationExpr.get(s"${aggCall.toString}#$running") match  {
        case Some(expr) =>
          expr

        case None =>
          val exp: GeneratedExpression = generateAggAccess(aggCall)
          aggregates += aggCall
          reusableAggregationExpr(s"${aggCall.toString}#$running") = exp
          addReusablePerRecordStatement(exp.code)
          exp.copy(code = NO_CODE)
      }
    }

    private def generateAggAccess(aggCall: RexCall): GeneratedExpression = {
      val singleAggResultTerm = newName("result")
      val singleAggNullTerm = newName("nullTerm")
      val singleAggResultType = FlinkTypeFactory.toTypeInfo(aggCall.`type`).toInternalType
      val primitiveSingleAggResultTypeTerm = primitiveTypeTermForType(singleAggResultType)
      val boxedSingleAggResultTypeTerm = boxedTypeTermForType(singleAggResultType)

      val allAggRowTerm = s"aggRow_$variableUID"

      val rowsForVariableCode = findEventsByPatternName(variable)
      val codeForAgg =
        j"""
           |$GENERIC_ROW $allAggRowTerm = $calculateAggFuncName(${rowsForVariableCode.resultTerm});
           |""".stripMargin

      addReusablePerRecordStatement(codeForAgg)

      val defaultValue = primitiveDefaultValue(singleAggResultType)
      val codeForSingleAgg = if (nullCheck) {
        j"""
           |boolean $singleAggNullTerm;
           |$primitiveSingleAggResultTypeTerm $singleAggResultTerm;
           |if ($allAggRowTerm.getField(${aggregates.size}) != null) {
           |  $singleAggResultTerm = ($boxedSingleAggResultTypeTerm) $allAggRowTerm
           |    .getField(${aggregates.size});
           |  $singleAggNullTerm = false;
           |} else {
           |  $singleAggNullTerm = true;
           |  $singleAggResultTerm = $defaultValue;
           |}
           |""".stripMargin
      } else {
        j"""
           |$primitiveSingleAggResultTypeTerm $singleAggResultTerm =
           |    ($boxedSingleAggResultTypeTerm) $allAggRowTerm.getField(${aggregates.size});
           |""".stripMargin
      }

      addReusablePerRecordStatement(codeForSingleAgg)

      GeneratedExpression(singleAggResultTerm, singleAggNullTerm, NO_CODE, singleAggResultType)
    }

    def generateAggFunction(): Unit = {
      val matchAgg = extractAggregatesAndExpressions

      val aggCalls = matchAgg.aggregations.map(a => AggregateCall.create(
        a.sqlAggFunction,
        false,
        false,
        a.exprIndices,
        -1,
        a.resultType,
        a.sqlAggFunction.getName))

      val needRetraction = matchAgg.aggregations.map(_ => false).toArray

      val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
      val inputRelType = typeFactory.createStructType(
        matchAgg.inputExprs.map(_.getType),
        matchAgg.inputExprs.indices.map(i => s"TMP$i"))

      val aggInfoList = AggregateUtil.transformToStreamAggregateInfoList(
        aggCalls,
        inputRelType,
        needRetraction,
        needInputCount = false,
        isStateBackendDataViews = false,
        needDistinctInfo = false)

      val inputFieldTypes = matchAgg.inputExprs
        .map(expr => FlinkTypeFactory.toInternalType(expr.getType))

      val aggsHandlerCodeGenerator = new AggsHandlerCodeGenerator(
        CodeGeneratorContext(new TableConfig, supportReference = true),
        relBuilder,
        inputFieldTypes,
        needRetract = false,
        needMerge = false,
        nullCheck = true,
        copyInputField = false)
      val generatedAggsHandler = aggsHandlerCodeGenerator.generateAggsHandler(
        s"AggFunction_$variableUID",
        aggInfoList)

      val generatedTerm = ctx.addReusableObject(generatedAggsHandler, "generatedAggHandler")
      val functionTerm = s"aggregator_$variableUID"

      val declareCode = s"private $AGGS_HANDLE_FUNCTION $functionTerm;"
      val initCode = s"$functionTerm = ($AGGS_HANDLE_FUNCTION) " +
        s"$generatedTerm.newInstance($CURRENT_CLASS_LOADER);"
      ctx.addReusableMember(declareCode, initCode)

      val transformFuncName = s"transformRowForAgg_$variableUID"
      val inputTransform: String = generateAggInputExprEvaluation(
        matchAgg.inputExprs,
        transformFuncName)

      generateAggCalculation(functionTerm, transformFuncName, inputTransform)
    }

    private def extractAggregatesAndExpressions: MatchAgg = {
      val inputRows = new mutable.LinkedHashMap[String, (RexNode, Int)]

      val singleAggregates = aggregates.map { aggCall =>
        val callsWithIndices = aggCall.operands.asScala.map(innerCall => {
          inputRows.get(innerCall.toString) match {
            case Some(x) =>
              x

            case None =>
              val callWithIndex = (innerCall, inputRows.size)
              inputRows(innerCall.toString) = callWithIndex
              callWithIndex
          }
        })

        SingleAggCall(
          aggCall.getOperator.asInstanceOf[SqlAggFunction],
          aggCall.`type`,
          callsWithIndices.map(callsWithIndice => Integer.valueOf(callsWithIndice._2)))
      }

      MatchAgg(singleAggregates, inputRows.values.map(_._1).toSeq)
    }

    private def generateAggCalculation(
        functionTerm: String,
        transformFuncName: String,
        inputTransformFunc: String): Unit = {
      val code =
        j"""
           |$inputTransformFunc
           |
           |private $GENERIC_ROW $calculateAggFuncName(java.util.List input)
           |    throws Exception {
           |  $functionTerm.setAccumulators($functionTerm.createAccumulators());
           |  for ($BASE_ROW row : input) {
           |    $functionTerm.accumulate($transformFuncName(row));
           |  }
           |  $GENERIC_ROW result = ($GENERIC_ROW) $functionTerm.getValue();
           |  return result;
           |}
           |""".stripMargin

      ctx.addReusableMember(code)

      ctx.addReusableOpenStatement(
        s"$functionTerm.open(new $EXECUTION_CONTEXT_IMPL(null, getRuntimeContext()));")
      ctx.addReusableCloseStatement(s"$functionTerm.close();")
    }

    private def generateAggInputExprEvaluation(
        inputExprs: Seq[RexNode],
        funcName: String): String = {
      isWithinAggExprState = true
      val resultTerm = newName("result")
      val exprs = inputExprs.zipWithIndex.map {
        case (inputExpr, outputIndex) =>
          val expr = generateExpression(inputExpr)
          s"""
             |  ${expr.code}
             |  if (${expr.nullTerm}) {
             |    $resultTerm.update($outputIndex, null);
             |  } else {
             |    $resultTerm.update($outputIndex, ${expr.resultTerm});
             |  }
         """.stripMargin
      }.mkString("\n")
      isWithinAggExprState = false

      j"""
         |private $GENERIC_ROW $funcName($BASE_ROW $inputAggRowTerm) {
         |  $GENERIC_ROW $resultTerm = new $GENERIC_ROW(${inputExprs.size});
         |  $exprs
         |  return $resultTerm;
         |}
         |""".stripMargin
    }

    private case class SingleAggCall(
        sqlAggFunction: SqlAggFunction,
        resultType: RelDataType,
        exprIndices: Seq[Integer]
    )

    private case class MatchAgg(
        aggregations: Seq[SingleAggCall],
        inputExprs: Seq[RexNode]
    )
  }
}

object MatchCodeGenerator {
  val ALL_PATTERN_VARIABLE = "*"

  val EXECUTION_CONTEXT_IMPL: String = className[ExecutionContextImpl]

  val AGGS_HANDLE_FUNCTION: String = className[AggsHandleFunction]

  val GENERIC_ROW: String = className[GenericRow]

  val BASE_ROW: String = className[BaseRow]

  val CURRENT_CLASS_LOADER = "Thread.currentThread().getContextClassLoader()"

  def generateProctimeTimestamp(): GeneratedExpression = {
    val resultTerm = newName("result")
    val resultCode =
      s"""
         |long $resultTerm = System.currentTimeMillis();
         |""".stripMargin.trim
    GeneratedExpression(resultTerm, NEVER_NULL, resultCode, DataTypes.TIMESTAMP)
  }
}
