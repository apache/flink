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

import java.lang.reflect.Modifier
import java.lang.{Iterable => JIterable}
import java.util.{List => JList}

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex.RexLiteral
import org.apache.flink.api.common.state.{ListStateDescriptor, MapStateDescriptor, State, StateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.typeutils.TypeExtractionUtils.{extractTypeArgument, getRawClass}
import org.apache.flink.table.api.{TableConfig, ValidationException}
import org.apache.flink.table.api.dataview._
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenUtils.{newName, reflectiveFieldWriteAccess}
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.dataview.{StateListView, StateMapView}
import org.apache.flink.table.functions.{TableAggregateFunction, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.flink.table.functions.aggfunctions.DistinctAccumulator
import org.apache.flink.table.functions.utils.{AggSqlFunction, UserDefinedFunctionUtils}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{getUserDefinedMethod, signatureToString}
import org.apache.flink.table.runtime.CRowWrappingCollector
import org.apache.flink.table.runtime.aggregate.AggregateUtil.CalcitePair
import org.apache.flink.table.runtime.aggregate.{AggregateUtil, GeneratedAggregations, GeneratedTableAggregations, SingleElementIterable}
import org.apache.flink.table.utils.EncodingUtils
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * A code generator for generating [[GeneratedAggregations]] or
  * [[GeneratedTableAggregations]].
  *
  * @param config                 configuration that determines runtime behavior
  * @param nullableInput          input(s) can be null.
  * @param inputTypeInfo          type information about the input of the Function
  * @param constants              constant expressions that act like a second input in the
  *                               parameter indices.
  * @param classNamePrefix        Class name of the function.
  *                               Does not need to be unique but has to be a valid Java class
  *                               identifier.
  * @param physicalInputTypes     Physical input row types
  * @param aggregates             All aggregate functions
  * @param aggFields              Indexes of the input fields for all aggregate functions
  * @param aggMapping             The mapping of aggregates to output fields
  * @param distinctAccMapping     The mapping of the distinct accumulator index to the
  *                               corresponding aggregates.
  * @param isStateBackedDataViews a flag to indicate if distinct filter uses state backend.
  * @param partialResults         A flag defining whether final or partial results (accumulators)
  *                               are set
  *                               to the output row.
  * @param fwdMapping             The mapping of input fields to output fields
  * @param mergeMapping           An optional mapping to specify the accumulators to merge. If not
  *                               set, we
  *                               assume that both rows have the accumulators at the same position.
  * @param outputArity            The number of fields in the output row.
  * @param needRetract            a flag to indicate if the aggregate needs the retract method
  * @param needMerge              a flag to indicate if the aggregate needs the merge method
  * @param needReset              a flag to indicate if the aggregate needs the resetAccumulator
  *                               method
  * @param accConfig              Data view specification for accumulators
  */
class AggregationCodeGenerator(
  config: TableConfig,
  nullableInput: Boolean,
  inputTypeInfo: TypeInformation[_ <: Any],
  constants: Option[Seq[RexLiteral]],
  classNamePrefix: String,
  physicalInputTypes: Seq[TypeInformation[_]],
  aggregates: Array[UserDefinedAggregateFunction[_ <: Any, _ <: Any]],
  aggFields: Array[Array[Int]],
  aggMapping: Array[Int],
  distinctAccMapping: Array[(Integer, JList[Integer])],
  isStateBackedDataViews: Boolean,
  partialResults: Boolean,
  fwdMapping: Array[Int],
  mergeMapping: Option[Array[Int]],
  outputArity: Int,
  needRetract: Boolean,
  needMerge: Boolean,
  needReset: Boolean,
  accConfig: Option[Array[Seq[DataViewSpec[_]]]])
  extends CodeGenerator(config, nullableInput, inputTypeInfo) {

  // set of statements for cleanup dataview that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableCleanupStatements = mutable.LinkedHashSet[String]()

  // get unique function name
  val funcName = newName(classNamePrefix)

  // register UDAGGs
  val aggs = aggregates.map(a => addReusableFunction(a, contextTerm))

  // get java types of accumulators
  val accTypeClasses = aggregates.map { a =>
    a.getClass.getMethod("createAccumulator").getReturnType
  }
  val accTypes = accTypeClasses.map(_.getCanonicalName)

  var parametersCode: Array[String] = _
  var parametersCodeForDistinctAcc: Array[String] = _
  var parametersCodeForDistinctMerge: Array[String] = _

  // get distinct filter of acc fields for each aggregate functions
  val distinctAccType = s"${classOf[DistinctAccumulator].getName}"
  var distinctAccCount: Int = _

  // create constants
  val constantExprs = constants.map(_.map(generateExpression)).getOrElse(Seq())
  val constantTypes = constantExprs.map(_.resultType)
  val constantFields = constantExprs.map(addReusableBoxedConstant)

  val isTableAggregate = AggregateUtil.containsTableAggregateFunction(aggregates)

  /**
    * @return code block of statements that need to be placed in the cleanup() method of
    *         [[GeneratedAggregations]]
    */
  def reuseCleanupCode(): String = {
    reusableCleanupStatements.mkString("", "\n", "\n")
  }

  def init(): Unit = {

    // get parameter lists for aggregation functions
    parametersCode = aggFields.map { inFields =>
      val fields = inFields.filter(_ > -1).map { f =>
        // index to constant
        if (f >= physicalInputTypes.length) {
          constantFields(f - physicalInputTypes.length)
        }
        // index to input field
        else {
          s"(${CodeGenUtils.boxedTypeTermForTypeInfo(physicalInputTypes(f))}) input.getField($f)"
        }
      }

      fields.mkString(", ")
    }

    // get parameter lists for distinct acc, constant fields are not necessary
    parametersCodeForDistinctAcc = aggFields.map { inFields =>
      val fields = inFields.filter(i => i > -1 && i < physicalInputTypes.length).map { f =>
        // index to input field
        s"(${CodeGenUtils.boxedTypeTermForTypeInfo(physicalInputTypes(f))}) input.getField($f)"
      }

      fields.mkString(", ")
    }

    parametersCodeForDistinctMerge = aggFields.map { inFields =>
      // transform inFields to pairs of (inField, index in acc) firstly,
      // e.g. (4, 2, 3, 2) will be transformed to ((4,2), (2,0), (3,1), (2,0))
      val fields = inFields.filter(_ > -1).groupBy(identity).toSeq.sortBy(_._1).zipWithIndex
        .flatMap { case (a, i) => a._2.map((_, i)) }
        .map { case (f, i) =>
          // index to constant
          if (f >= physicalInputTypes.length) {
            constantFields(f - physicalInputTypes.length)
          }
          // index to input field
          else {
            s"(${CodeGenUtils.boxedTypeTermForTypeInfo(physicalInputTypes(f))}) k.getField($i)"
          }
        }

      fields.mkString(", ")
    }

    // get method signatures
    val classes = UserDefinedFunctionUtils.typeInfoToClass(physicalInputTypes)
    val constantClasses = UserDefinedFunctionUtils.typeInfoToClass(constantTypes)
    val methodSignaturesList = aggFields.map { inFields =>
      inFields.filter(_ > -1).map { f =>
        // index to constant
        if (f >= physicalInputTypes.length) {
          constantClasses(f - physicalInputTypes.length)
        }
        // index to input field
        else {
          classes(f)
        }
      }
    }

    distinctAccCount = distinctAccMapping.count(_._1 >= 0)
    if (distinctAccCount > 0 && partialResults && isStateBackedDataViews) {
      // should not happen, but add an error message just in case.
      throw new CodeGenException(
        s"Cannot emit partial results if DISTINCT values are tracked in state-backed maps. " +
          s"Please report this bug."
      )
    }

    // initialize and create data views for accumulators & distinct filters
    addAccumulatorDataViews()

    // check and validate the needed methods
    aggregates.zipWithIndex.map {
      case (a, i) =>
        getUserDefinedMethod(a, "accumulate", Array(accTypeClasses(i)) ++ methodSignaturesList(i))
          .getOrElse(
            throw new CodeGenException(
              s"No matching accumulate method found for AggregateFunction " +
                s"'${a.getClass.getCanonicalName}'" +
                s"with parameters '${signatureToString(methodSignaturesList(i))}'.")
          )

        if (needRetract) {
          getUserDefinedMethod(a, "retract", Array(accTypeClasses(i)) ++ methodSignaturesList(i))
            .getOrElse(
              throw new CodeGenException(
                s"No matching retract method found for AggregateFunction " +
                  s"'${a.getClass.getCanonicalName}'" +
                  s"with parameters '${signatureToString(methodSignaturesList(i))}'.")
            )
        }

        if (needMerge) {
          val method =
            getUserDefinedMethod(a, "merge", Array(accTypeClasses(i), classOf[JIterable[Any]]))
              .getOrElse(
                throw new CodeGenException(
                  s"No matching merge method found for AggregateFunction " +
                    s"${a.getClass.getCanonicalName}'.")
              )

          // use the TypeExtractionUtils here to support nested GenericArrayTypes and
          // other complex types
          val iterableGenericType = extractTypeArgument(method.getGenericParameterTypes()(1), 0)
          val iterableTypeClass = getRawClass(iterableGenericType)

          if (iterableTypeClass != accTypeClasses(i)) {
            throw new CodeGenException(
              s"Merge method in AggregateFunction ${a.getClass.getCanonicalName} does not have " +
                s"the correct Iterable type. Actually: ${iterableTypeClass.toString}. " +
                s"Expected: ${accTypeClasses(i).toString}")
          }
        }

        if (needReset) {
          getUserDefinedMethod(a, "resetAccumulator", Array(accTypeClasses(i)))
            .getOrElse(
              throw new CodeGenException(
                s"No matching resetAccumulator method found for " +
                  s"aggregate ${a.getClass.getCanonicalName}'.")
            )
        }
    }
  }

  /**
    * Add all data views for all field accumulators and distinct filters defined by
    * aggregation functions.
    */
  def addAccumulatorDataViews(): Unit = {
    if (accConfig.isDefined) {
      // create state handles for DataView backed accumulator fields.
      val descMapping: Map[String, StateDescriptor[_, _]] = accConfig.get
        .flatMap(specs => specs.map(s => (s.stateId, s.toStateDescriptor)))
        .toMap[String, StateDescriptor[_ <: State, _]]

      for (i <- 0 until aggs.length + distinctAccCount) yield {
        for (spec <- accConfig.get(i)) yield {
          // Check if stat descriptor exists.
          val desc: StateDescriptor[_, _] = descMapping.getOrElse(spec.stateId,
            throw new CodeGenException(
              s"Can not find DataView in accumulator by id: ${spec.stateId}"))

          addReusableDataView(spec, desc, i)
        }
      }
    }
  }

  /**
    * Create DataView Term, for example, acc1_map_dataview.
    *
    * @param aggIndex index of aggregate function
    * @param fieldName field name of DataView
    * @return term to access [[MapView]] or [[ListView]]
    */
  def createDataViewTerm(aggIndex: Int, fieldName: String): String = {
    s"acc${aggIndex}_${fieldName}_dataview"
  }

  /**
    * Adds a reusable [[org.apache.flink.table.api.dataview.DataView]] to the open, cleanup,
    * close and member area of the generated function.
    * @param spec the [[DataViewSpec]] of the desired data view term.
    * @param desc the [[StateDescriptor]] of the desired data view term.
    * @param aggIndex the aggregation function index associate with the data view.
    */
  def addReusableDataView(
    spec: DataViewSpec[_],
    desc: StateDescriptor[_, _],
    aggIndex: Int): Unit = {
    val dataViewField = spec.field
    val dataViewTypeTerm = dataViewField.getType.getCanonicalName

    // define the DataView variables
    val serializedData = EncodingUtils.encodeObjectToString(desc)
    val dataViewFieldTerm = createDataViewTerm(aggIndex, dataViewField.getName)
    val field =
      s"""
         |    final $dataViewTypeTerm $dataViewFieldTerm;
         |""".stripMargin
    reusableMemberStatements.add(field)

    // create DataViews
    val descFieldTerm = s"${dataViewFieldTerm}_desc"
    val descClassQualifier = classOf[StateDescriptor[_, _]].getCanonicalName
    val descDeserializeCode =
      s"""
         |    $descClassQualifier $descFieldTerm = ($descClassQualifier)
         |      ${classOf[EncodingUtils].getCanonicalName}.decodeStringToObject(
         |        "$serializedData",
         |        $descClassQualifier.class,
         |        $contextTerm.getUserCodeClassLoader());
         |""".stripMargin
    val createDataView = if (dataViewField.getType == classOf[MapView[_, _]]) {
      s"""
         |    $descDeserializeCode
         |    $dataViewFieldTerm = new ${classOf[StateMapView[_, _]].getCanonicalName}(
         |      $contextTerm.getMapState(
         |        (${classOf[MapStateDescriptor[_, _]].getCanonicalName}) $descFieldTerm));
         |""".stripMargin
    } else if (dataViewField.getType == classOf[ListView[_]]) {
      s"""
         |    $descDeserializeCode
         |    $dataViewFieldTerm = new ${classOf[StateListView[_]].getCanonicalName}(
         |      $contextTerm.getListState(
         |        (${classOf[ListStateDescriptor[_]].getCanonicalName}) $descFieldTerm));
         |""".stripMargin
    } else {
      throw new CodeGenException(s"Unsupported dataview type: $dataViewTypeTerm")
    }
    reusableOpenStatements.add(createDataView)

    // cleanup DataViews
    val cleanup =
      s"""
         |    $dataViewFieldTerm.clear();
         |""".stripMargin
    reusableCleanupStatements.add(cleanup)
  }

  def genAccDataViewFieldSetter(str: String, i: Int): String = {
    if (accConfig.isDefined) {
      genDataViewFieldSetter(accConfig.get(i), str, i)
    } else {
      ""
    }
  }

  /**
    * Generate statements to set data view field when use state backend.
    *
    * @param specs aggregation [[DataViewSpec]]s for this aggregation term.
    * @param accTerm aggregation term
    * @param aggIndex index of aggregation
    * @return data view field set statements
    */
  def genDataViewFieldSetter(
    specs: Seq[DataViewSpec[_]],
    accTerm: String,
    aggIndex: Int): String = {
    val setters = for (spec <- specs) yield {
      val field = spec.field
      val dataViewTerm = createDataViewTerm(aggIndex, field.getName)
      val fieldSetter = if (Modifier.isPublic(field.getModifiers)) {
        s"$accTerm.${field.getName} = $dataViewTerm;"
      } else {
        val fieldTerm = addReusablePrivateFieldAccess(field.getDeclaringClass, field.getName)
        s"${reflectiveFieldWriteAccess(fieldTerm, field, accTerm, dataViewTerm)};"
      }

      s"""
         |    $fieldSetter
          """.stripMargin
    }
    setters.mkString("\n")
  }

  def genSetAggregationResults: String = {

    val sig: String =
      j"""
         |  public final void setAggregationResults(
         |    org.apache.flink.types.Row accs,
         |    org.apache.flink.types.Row output) throws Exception """.stripMargin

    val setAggs: String = {
      for ((i, aggIndexes) <- distinctAccMapping) yield {
        if (partialResults) {
          def setAggs(aggIndexes: JList[Integer]) = {
            for (i <- aggIndexes) yield {
              j"""
                 |output.setField(
                 |  ${aggMapping(i)},
                 |  (${accTypes(i)}) accs.getField($i));
                 """.stripMargin
            }
          }.mkString("\n")

          if (i >= 0) {
            j"""
               |    output.setField(
               |      ${aggMapping(i)},
               |      ($distinctAccType) accs.getField($i));
               |    ${setAggs(aggIndexes)}
                 """.stripMargin
          } else {
            j"""
               |    ${setAggs(aggIndexes)}
                 """.stripMargin
          }
        } else {
          def setAggs(aggIndexes: JList[Integer]) = {
            for (i <- aggIndexes) yield {
              val setAccOutput =
                j"""
                   |${genAccDataViewFieldSetter(s"acc$i", i)}
                   |output.setField(
                   |  ${aggMapping(i)},
                   |  baseClass$i.getValue(acc$i));
                     """.stripMargin

              j"""
                 |org.apache.flink.table.functions.AggregateFunction baseClass$i =
                 |  (org.apache.flink.table.functions.AggregateFunction) ${aggs(i)};
                 |${accTypes(i)} acc$i = (${accTypes(i)}) accs.getField($i);
                 |$setAccOutput
                   """.stripMargin
            }
          }.mkString("\n")

          j"""
             |    ${setAggs(aggIndexes)}
               """.stripMargin
        }
      }
    }.mkString("\n")

    j"""
       |$sig {
       |$setAggs
       |  }""".stripMargin
  }

  def genAccumulate: String = {

    val sig: String =
      j"""
         |  public final void accumulate(
         |    org.apache.flink.types.Row accs,
         |    org.apache.flink.types.Row input) throws Exception """.stripMargin

    val accumulate: String = {
      def accumulateAcc(aggIndexes: JList[Integer]) = {
        for (i <- aggIndexes) yield {
          j"""
             |${accTypes(i)} acc$i = (${accTypes(i)}) accs.getField($i);
             |${genAccDataViewFieldSetter(s"acc$i", i)}
             |${aggs(i)}.accumulate(acc$i
             |  ${if (!parametersCode(i).isEmpty) "," else ""} ${parametersCode(i)});
               """.stripMargin
        }
      }.mkString("\n")

      for ((i, aggIndexes) <- distinctAccMapping) yield {
        if (i >= 0) {
          j"""
             |    $distinctAccType distinctAcc$i = ($distinctAccType) accs.getField($i);
             |    ${genAccDataViewFieldSetter(s"distinctAcc$i", i)}
             |    if (distinctAcc$i.add(${classOf[Row].getCanonicalName}.of(
             |        ${parametersCodeForDistinctAcc(aggIndexes.get(0))}))) {
             |      ${accumulateAcc(aggIndexes)}
             |    }
               """.stripMargin
        } else {
          j"""
             |    ${accumulateAcc(aggIndexes)}
               """.stripMargin
        }
      }
    }.mkString("\n")

    j"""$sig {
       |$accumulate
       |  }""".stripMargin
  }

  def genRetract: String = {

    val sig: String =
      j"""
         |  public final void retract(
         |    org.apache.flink.types.Row accs,
         |    org.apache.flink.types.Row input) throws Exception """.stripMargin

    val retract: String = {
      def retractAcc(aggIndexes: JList[Integer]) = {
        for (i <- aggIndexes) yield {
          j"""
             |${accTypes(i)} acc$i = (${accTypes(i)}) accs.getField($i);
             |${genAccDataViewFieldSetter(s"acc$i", i)}
             |${aggs(i)}.retract(acc$i
             |  ${if (!parametersCode(i).isEmpty) "," else ""} ${parametersCode(i)});
               """.stripMargin
        }
      }.mkString("\n")

      for ((i, aggIndexes) <- distinctAccMapping) yield {
        if (i >= 0) {
          j"""
             |    $distinctAccType distinctAcc$i = ($distinctAccType) accs.getField($i);
             |    ${genAccDataViewFieldSetter(s"distinctAcc$i", i)}
             |    if (distinctAcc$i.remove(${classOf[Row].getCanonicalName}.of(
             |        ${parametersCodeForDistinctAcc(aggIndexes.get(0))}))) {
             |      ${retractAcc(aggIndexes)}
             |    }
               """.stripMargin
        } else {
          j"""
             |    ${retractAcc(aggIndexes)}
               """.stripMargin
        }
      }
    }.mkString("\n")

    if (needRetract) {
      j"""
         |$sig {
         |$retract
         |  }""".stripMargin
    } else {
      j"""
         |$sig {
         |  }""".stripMargin
    }
  }

  def genCreateAccumulators: String = {

    val sig: String =
      j"""
         |  public final org.apache.flink.types.Row createAccumulators() throws Exception
         |    """.stripMargin
    val init: String =
      j"""
         |      org.apache.flink.types.Row accs =
         |          new org.apache.flink.types.Row(${aggs.length + distinctAccCount});"""
        .stripMargin
    val create: String = {
      def createAcc(aggIndexes: JList[Integer]) = {
        for (i <- aggIndexes) yield {
          j"""
             |${accTypes(i)} acc$i = (${accTypes(i)}) ${aggs(i)}.createAccumulator();
             |accs.setField(
             |  $i,
             |  acc$i);
               """.stripMargin
        }
      }.mkString("\n")

      for ((i, aggIndexes) <- distinctAccMapping) yield {
        if (i >= 0) {
          j"""
             |    $distinctAccType distinctAcc$i = ($distinctAccType)
             |      new ${classOf[DistinctAccumulator].getCanonicalName}();
             |    accs.setField(
             |      $i,
             |      distinctAcc$i);
             |    ${createAcc(aggIndexes)}
               """.stripMargin
        } else {
          j"""
             |    ${createAcc(aggIndexes)}
               """.stripMargin
        }
      }
    }.mkString("\n")
    val ret: String =
      j"""
         |      return accs;"""
        .stripMargin

    j"""$sig {
       |$init
       |$create
       |$ret
       |  }""".stripMargin
  }

  def genSetForwardedFields: String = {

    val sig: String =
      j"""
         |  public final void setForwardedFields(
         |    org.apache.flink.types.Row input,
         |    org.apache.flink.types.Row output)
         |    """.stripMargin

    val forward: String = {
      for (i <- fwdMapping.indices if fwdMapping(i) >= 0) yield
        {
          j"""
             |    output.setField(
             |      $i,
             |      input.getField(${fwdMapping(i)}));"""
            .stripMargin
        }
    }.mkString("\n")

    j"""$sig {
       |$forward
       |  }""".stripMargin
  }

  def genCreateOutputRow: String = {
    j"""
       |  public final org.apache.flink.types.Row createOutputRow() {
       |    return new org.apache.flink.types.Row($outputArity);
       |  }""".stripMargin
  }

  def genMergeAccumulatorsPair: String = {
    val mapping = mergeMapping.getOrElse((0 until aggs.length + distinctAccCount).toArray)

    val sig: String =
      j"""
         |  public final org.apache.flink.types.Row mergeAccumulatorsPair(
         |    org.apache.flink.types.Row a,
         |    org.apache.flink.types.Row b)
           """.stripMargin
    val merge: String = {
      def accumulateAcc(aggIndexes: JList[Integer]) = {
        for (i <- aggIndexes) yield {
          j"""
             |${accTypes(i)} aAcc$i = (${accTypes(i)}) a.getField($i);
             |${aggs(i)}.accumulate(aAcc$i, ${parametersCodeForDistinctMerge(i)});
             |a.setField($i, aAcc$i);
               """.stripMargin
        }
      }.mkString("\n")

      def mergeAcc(aggIndexes: JList[Integer]) = {
        for (i <- aggIndexes) yield {
          j"""
             |${accTypes(i)} aAcc$i = (${accTypes(i)}) a.getField($i);
             |${accTypes(i)} bAcc$i = (${accTypes(i)}) b.getField(${mapping(i)});
             |accIt$i.setElement(bAcc$i);
             |${aggs(i)}.merge(aAcc$i, accIt$i);
             |a.setField($i, aAcc$i);
               """.stripMargin
        }
      }.mkString("\n")

      for ((i, aggIndexes) <- distinctAccMapping) yield {
        if (i >= 0) {
          j"""
             |    $distinctAccType aDistinctAcc$i = ($distinctAccType) a.getField($i);
             |    $distinctAccType bDistinctAcc$i = ($distinctAccType) b.getField(${mapping(i)});
             |    java.util.Iterator<java.util.Map.Entry> mergeIt$i =
             |        bDistinctAcc$i.elements().iterator();
             |
               |    while (mergeIt$i.hasNext()) {
             |      java.util.Map.Entry entry = (java.util.Map.Entry) mergeIt$i.next();
             |      ${classOf[Row].getCanonicalName} k =
             |          (${classOf[Row].getCanonicalName}) entry.getKey();
             |      Long v = (Long) entry.getValue();
             |      if (aDistinctAcc$i.add(k, v)) {
             |        ${accumulateAcc(aggIndexes)}
             |      }
             |    }
             |    a.setField($i, aDistinctAcc$i);
               """.stripMargin
        } else {
          j"""
             |    ${mergeAcc(aggIndexes)}
               """.stripMargin
        }
      }
    }.mkString("\n")
    val ret: String =
      j"""
         |      return a;
           """.stripMargin

    if (needMerge) {
      if (accConfig.isDefined) {
        throw new CodeGenException("DataView doesn't support merge when the backend uses " +
          s"state when generate aggregation for $funcName.")
      }
      j"""
         |$sig {
         |$merge
         |$ret
         |  }""".stripMargin
    } else {
      j"""
         |$sig {
         |$ret
         |  }""".stripMargin
    }
  }

  def genMergeList: String = {
    {
      val singleIterableClass = classOf[SingleElementIterable[_]].getCanonicalName
      for (i <- accTypes.indices) yield
        j"""
           |    private final $singleIterableClass<${accTypes(i)}> accIt$i =
           |      new $singleIterableClass<${accTypes(i)}>();
             """.stripMargin
    }.mkString("\n")
  }

  def genResetAccumulator: String = {

    val sig: String =
      j"""
         |  public final void resetAccumulator(
         |    org.apache.flink.types.Row accs) throws Exception """.stripMargin

    val reset: String = {
      def resetAcc(aggIndexes: JList[Integer]) = {
        for (i <- aggIndexes) yield {
          j"""
             |${accTypes(i)} acc$i = (${accTypes(i)}) accs.getField($i);
             |${genAccDataViewFieldSetter(s"acc$i", i)}
             |${aggs(i)}.resetAccumulator(acc$i);
               """.stripMargin
        }
      }.mkString("\n")

      for ((i, aggIndexes) <- distinctAccMapping) yield {
        if (i >= 0) {
          j"""
             |    $distinctAccType distinctAcc$i = ($distinctAccType) accs.getField($i);
             |    ${genAccDataViewFieldSetter(s"distinctAcc$i", i)}
             |    distinctAcc$i.reset();
             |    ${resetAcc(aggIndexes)}
               """.stripMargin
        } else {
          j"""
             |    ${resetAcc(aggIndexes)}
               """.stripMargin
        }
      }
    }.mkString("\n")

    if (needReset) {
      j"""$sig {
         |$reset
         |  }""".stripMargin
    } else {
      j"""$sig {
         |  }""".stripMargin
    }
  }

  /**
    * Generates a [[GeneratedAggregations]] that can be passed to a Java compiler.
    *
    * @return A GeneratedAggregationsFunction
    */
  def generateAggregations: GeneratedAggregationsFunction = {

    init()
    val aggFuncCode = Seq(
      genSetAggregationResults,
      genAccumulate,
      genRetract,
      genCreateAccumulators,
      genSetForwardedFields,
      genCreateOutputRow,
      genMergeAccumulatorsPair,
      genResetAccumulator).mkString("\n")

    val generatedAggregationsClass = classOf[GeneratedAggregations].getCanonicalName
    val funcCode =
      j"""
         |public final class $funcName extends $generatedAggregationsClass {
         |
         |  ${reuseMemberCode()}
         |  $genMergeList
         |  public $funcName() throws Exception {
         |    ${reuseInitCode()}
         |  }
         |  ${reuseConstructorCode(funcName)}
         |
         |  public final void open(
         |    org.apache.flink.api.common.functions.RuntimeContext $contextTerm) throws Exception {
         |    ${reuseOpenCode()}
         |  }
         |
         |  $aggFuncCode
         |
         |  public final void cleanup() throws Exception {
         |    ${reuseCleanupCode()}
         |  }
         |
         |  public final void close() throws Exception {
         |    ${reuseCloseCode()}
         |  }
         |}
         """.stripMargin

    GeneratedAggregationsFunction(funcName, funcCode)
  }

  /**
    * Generates a [[org.apache.flink.table.runtime.aggregate.GeneratedAggregations]] that can be
    * passed to a Java compiler.
    *
    * @return A GeneratedAggregationsFunction
    */
  def generateTableAggregations(
    tableAggOutputRowType: RowTypeInfo,
    tableAggOutputType: TypeInformation[_],
    supportEmitIncrementally: Boolean): GeneratedAggregationsFunction = {

    // constants
    val CONVERT_COLLECTOR_CLASS_TERM = "ConvertCollector"
    val CONVERT_COLLECTOR_VARIABLE_TERM = "convertCollector"
    val COLLECTOR_VARIABLE_TERM = "cRowWrappingcollector"
    val CONVERTER_ROW_RESULT_TERM = "rowTerm"

    // emit methods
    val emitValue = "emitValue"
    val emitUpdateWithRetract = "emitUpdateWithRetract"

    // collectors
    val COLLECTOR: String = classOf[Collector[_]].getCanonicalName
    val CROW_WRAPPING_COLLECTOR: String = classOf[CRowWrappingCollector].getCanonicalName
    val RETRACTABLE_COLLECTOR: String =
      classOf[TableAggregateFunction.RetractableCollector[_]].getCanonicalName

    val ROW: String = classOf[Row].getCanonicalName

    // Set emitValue as the default emit method here and set it to emitUpdateWithRetract on
    // condition that: 1. emitUpdateWithRetract has been defined in the table aggregate
    // function and 2. the operator supports emit incrementally, for example, window flatAggregate
    // doesn't support emit incrementally now)
    var finalEmitMethodName: String = emitValue

    def genEmit: String = {

      val sig: String =
        j"""
           |  public final void emit(
           |    $ROW accs,
           |    $COLLECTOR<$ROW> collector) throws Exception """.stripMargin

      val emit: String = {
        for (i <- aggs.indices) yield {
          val emitAcc =
            j"""
               |      ${genAccDataViewFieldSetter(s"acc$i", i)}
               |      ${aggs(i)}.$finalEmitMethodName(acc$i
               |        ${if (!parametersCode(i).isEmpty) "," else ""}
               |        $CONVERT_COLLECTOR_VARIABLE_TERM);
             """.stripMargin
          j"""
             |    ${accTypes(i)} acc$i = (${accTypes(i)}) accs.getField($i);
             |    $CONVERT_COLLECTOR_VARIABLE_TERM.$COLLECTOR_VARIABLE_TERM =
             |      ($CROW_WRAPPING_COLLECTOR) collector;
             |    $emitAcc
           """.stripMargin
        }
      }.mkString("\n")

      j"""$sig {
         |$emit
         |}""".stripMargin
    }

    def genRecordToRow: String = {
      // gen access expr

      val functionGenerator = new FunctionCodeGenerator(
        config,
        false,
        tableAggOutputType,
        None,
        None,
        None)

      functionGenerator.outRecordTerm = s"$CONVERTER_ROW_RESULT_TERM"
      val resultExprs = functionGenerator.generateConverterResultExpression(
        tableAggOutputRowType, tableAggOutputRowType.getFieldNames)

      functionGenerator.reuseInputUnboxingCode() + resultExprs.code
    }

    def checkAndGetEmitValueMethod(function: UserDefinedFunction, index: Int): Unit = {
      finalEmitMethodName = emitValue
      getUserDefinedMethod(
        function, emitValue, Array(accTypeClasses(index), classOf[Collector[_]]))
        .getOrElse(throw new CodeGenException(
          s"No matching $emitValue method found for " +
            s"tableAggregate ${function.getClass.getCanonicalName}'."))
    }

    /**
      * Call super init and check emit methods.
      */
    def innerInit(): Unit = {
      init()
      // check and validate the emit methods. Find incremental emit method first if the operator
      // supports emit incrementally.
      aggregates.zipWithIndex.map {
        case (a, i) =>
          if (supportEmitIncrementally) {
            try {
              finalEmitMethodName = emitUpdateWithRetract
              getUserDefinedMethod(
                a,
                emitUpdateWithRetract,
                Array(accTypeClasses(i), classOf[TableAggregateFunction.RetractableCollector[_]]))
                .getOrElse(checkAndGetEmitValueMethod(a, i))
            } catch {
              case _: ValidationException =>
                // Use try catch here as exception will be thrown if there is no
                // emitUpdateWithRetract method
                checkAndGetEmitValueMethod(a, i)
            }
          } else {
            checkAndGetEmitValueMethod(a, i)
          }
      }
    }

    /**
      * Generates the retract method if it is a [[TableAggregateFunction.RetractableCollector]].
      */
    def getRetractMethodForConvertCollector(emitMethodName: String): String = {
      if (emitMethodName == emitValue) {
        // Users can't retract messages with emitValue method.
        j"""
           |
          """.stripMargin
      } else {
        // only generates retract method for RetractableCollector
        j"""
           |      @Override
           |      public void retract(Object record) throws Exception {
           |          $COLLECTOR_VARIABLE_TERM.setChange(false);
           |          $COLLECTOR_VARIABLE_TERM.collect(convertToRow(record));
           |          $COLLECTOR_VARIABLE_TERM.setChange(true);
           |      }
          """.stripMargin
      }
    }

    innerInit()
    val aggFuncCode = Seq(
      genAccumulate,
      genRetract,
      genCreateAccumulators,
      genCreateOutputRow,
      genSetForwardedFields,
      genMergeAccumulatorsPair,
      genEmit).mkString("\n")

    val generatedAggregationsClass = classOf[GeneratedTableAggregations].getCanonicalName
    val aggOutputTypeName = tableAggOutputType.getTypeClass.getCanonicalName

    val baseCollectorString =
      if (finalEmitMethodName == emitValue) COLLECTOR else RETRACTABLE_COLLECTOR

    val funcCode =
      j"""
         |public final class $funcName extends $generatedAggregationsClass {
         |
         |  private $CONVERT_COLLECTOR_CLASS_TERM $CONVERT_COLLECTOR_VARIABLE_TERM;
         |  ${reuseMemberCode()}
         |  $genMergeList
         |  public $funcName() throws Exception {
         |    ${reuseInitCode()}
         |    $CONVERT_COLLECTOR_VARIABLE_TERM = new $CONVERT_COLLECTOR_CLASS_TERM();
         |  }
         |  ${reuseConstructorCode(funcName)}
         |
         |  public final void open(
         |    org.apache.flink.api.common.functions.RuntimeContext $contextTerm) throws Exception {
         |    ${reuseOpenCode()}
         |  }
         |
         |  $aggFuncCode
         |
         |  public final void cleanup() throws Exception {
         |    ${reuseCleanupCode()}
         |  }
         |
         |  public final void close() throws Exception {
         |    ${reuseCloseCode()}
         |  }
         |
         |  private class $CONVERT_COLLECTOR_CLASS_TERM implements $baseCollectorString {
         |
         |      public $CROW_WRAPPING_COLLECTOR $COLLECTOR_VARIABLE_TERM;
         |      private final $ROW $CONVERTER_ROW_RESULT_TERM =
         |        new $ROW(${tableAggOutputType.getArity});
         |
         |      public $ROW convertToRow(Object record) throws Exception {
         |         $aggOutputTypeName in1 = ($aggOutputTypeName) record;
         |         $genRecordToRow
         |         return $CONVERTER_ROW_RESULT_TERM;
         |      }
         |
         |      @Override
         |      public void collect(Object record) throws Exception {
         |          $COLLECTOR_VARIABLE_TERM.collect(convertToRow(record));
         |      }
         |
         |      ${getRetractMethodForConvertCollector(finalEmitMethodName)}
         |
         |      @Override
         |      public void close() {
         |       $COLLECTOR_VARIABLE_TERM.close();
         |      }
         |  }
         |}
         """.stripMargin

    new GeneratedTableAggregationsFunction(funcName, funcCode, finalEmitMethodName != emitValue)
  }

  /**
    * Generates a [[GeneratedAggregations]] or a [[GeneratedTableAggregations]] that can be passed
    * to a Java compiler.
    *
    * @param outputType      Output type of the (table)aggregate node.
    * @param groupSize       The size of the groupings.
    * @param namedAggregates The correspond named aggregates in the aggregate operator.
    * @param supportEmitIncrementally Whether support emit values incrementally. Window operators
    *                                 don't support it yet.
    * @return A GeneratedAggregationsFunction
    */
  def genAggregationsOrTableAggregations(
    outputType: RelDataType,
    groupSize: Int,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    supportEmitIncrementally: Boolean)
  : GeneratedAggregationsFunction = {

    if (isTableAggregate) {
      // get the output row type of the table aggregate
      val sqlAggFunction = namedAggregates.head.left.getAggregation.asInstanceOf[AggSqlFunction]
      val aggOutputArity = sqlAggFunction.returnType.getArity
      val tableAggOutputRowType = new RowTypeInfo(
        outputType.getFieldList
          .map(e => FlinkTypeFactory.toTypeInfo(e.getType))
          .slice(groupSize, groupSize + aggOutputArity)
          .toArray,
        outputType.getFieldNames
          .slice(groupSize, groupSize + aggOutputArity)
          .toArray)

      generateTableAggregations(
        tableAggOutputRowType,
        namedAggregates.head.left.getAggregation.asInstanceOf[AggSqlFunction].returnType,
        supportEmitIncrementally)
    } else {
      generateAggregations
    }
  }
}
