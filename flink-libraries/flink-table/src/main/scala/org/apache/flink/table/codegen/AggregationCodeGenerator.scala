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

import java.lang.reflect.ParameterizedType
import java.lang.{Iterable => JIterable}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.codegen.CodeGenUtils.newName
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{getUserDefinedMethod, signatureToString}
import org.apache.flink.table.runtime.aggregate.{GeneratedAggregations, SingleElementIterable}

/**
  * A code generator for generating [[GeneratedAggregations]].
  *
  * @param config configuration that determines runtime behavior
  * @param nullableInput input(s) can be null.
  * @param input type information about the input of the Function
  */
class AggregationCodeGenerator(
    config: TableConfig,
    nullableInput: Boolean,
    input: TypeInformation[_ <: Any])
  extends CodeGenerator(config, nullableInput, input) {

  /**
    * Generates a [[org.apache.flink.table.runtime.aggregate.GeneratedAggregations]] that can be
    * passed to a Java compiler.
    *
    * @param name        Class name of the function.
    *                    Does not need to be unique but has to be a valid Java class identifier.
    * @param generator   The code generator instance
    * @param physicalInputTypes Physical input row types
    * @param aggregates  All aggregate functions
    * @param aggFields   Indexes of the input fields for all aggregate functions
    * @param aggMapping  The mapping of aggregates to output fields
    * @param partialResults A flag defining whether final or partial results (accumulators) are set
    *                       to the output row.
    * @param fwdMapping  The mapping of input fields to output fields
    * @param mergeMapping An optional mapping to specify the accumulators to merge. If not set, we
    *                     assume that both rows have the accumulators at the same position.
    * @param constantFlags An optional parameter to define where to set constant boolean flags in
    *                      the output row.
    * @param outputArity The number of fields in the output row.
    * @param needRetract a flag to indicate if the aggregate needs the retract method
    * @param needMerge a flag to indicate if the aggregate needs the merge method
    * @param needReset a flag to indicate if the aggregate needs the resetAccumulator method
    *
    * @return A GeneratedAggregationsFunction
    */
  def generateAggregations(
    name: String,
    generator: CodeGenerator,
    physicalInputTypes: Seq[TypeInformation[_]],
    aggregates: Array[AggregateFunction[_ <: Any, _ <: Any]],
    aggFields: Array[Array[Int]],
    aggMapping: Array[Int],
    partialResults: Boolean,
    fwdMapping: Array[Int],
    mergeMapping: Option[Array[Int]],
    constantFlags: Option[Array[(Int, Boolean)]],
    outputArity: Int,
    needRetract: Boolean,
    needMerge: Boolean,
    needReset: Boolean)
  : GeneratedAggregationsFunction = {

    // get unique function name
    val funcName = newName(name)
    // register UDAGGs
    val aggs = aggregates.map(a => generator.addReusableFunction(a))
    // get java types of accumulators
    val accTypeClasses = aggregates.map { a =>
      a.getClass.getMethod("createAccumulator").getReturnType
    }
    val accTypes = accTypeClasses.map(_.getCanonicalName)

    // get java classes of input fields
    val javaClasses = physicalInputTypes.map(t => t.getTypeClass)
    // get parameter lists for aggregation functions
    val parameters = aggFields.map { inFields =>
      val fields = for (f <- inFields) yield
        s"(${javaClasses(f).getCanonicalName}) input.getField($f)"
      fields.mkString(", ")
    }
    val methodSignaturesList = aggFields.map {
      inFields => for (f <- inFields) yield javaClasses(f)
    }

    // check and validate the needed methods
    aggregates.zipWithIndex.map {
      case (a, i) => {
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
          val methods =
            getUserDefinedMethod(a, "merge", Array(accTypeClasses(i), classOf[JIterable[Any]]))
            .getOrElse(
              throw new CodeGenException(
                s"No matching merge method found for AggregateFunction " +
                  s"${a.getClass.getCanonicalName}'.")
            )

          var iterableTypeClass = methods.getGenericParameterTypes.apply(1)
                                  .asInstanceOf[ParameterizedType].getActualTypeArguments.apply(0)
          // further extract iterableTypeClass if the accumulator has generic type
          iterableTypeClass match {
            case impl: ParameterizedType => iterableTypeClass = impl.getRawType
            case _ =>
          }

          if (iterableTypeClass != accTypeClasses(i)) {
            throw new CodeGenException(
              s"merge method in AggregateFunction ${a.getClass.getCanonicalName} does not have " +
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

    def genSetAggregationResults: String = {

      val sig: String =
        j"""
           |  public final void setAggregationResults(
           |    org.apache.flink.types.Row accs,
           |    org.apache.flink.types.Row output)""".stripMargin

      val setAggs: String = {
        for (i <- aggs.indices) yield

          if (partialResults) {
            j"""
               |    output.setField(
               |      ${aggMapping(i)},
               |      (${accTypes(i)}) accs.getField($i));""".stripMargin
          } else {
            j"""
               |    org.apache.flink.table.functions.AggregateFunction baseClass$i =
               |      (org.apache.flink.table.functions.AggregateFunction) ${aggs(i)};
               |
               |    output.setField(
               |      ${aggMapping(i)},
               |      baseClass$i.getValue((${accTypes(i)}) accs.getField($i)));""".stripMargin
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
           |    org.apache.flink.types.Row input)""".stripMargin

      val accumulate: String = {
        for (i <- aggs.indices) yield
          j"""
             |    ${aggs(i)}.accumulate(
             |      ((${accTypes(i)}) accs.getField($i)),
             |      ${parameters(i)});""".stripMargin
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
           |    org.apache.flink.types.Row input)""".stripMargin

      val retract: String = {
        for (i <- aggs.indices) yield
          j"""
             |    ${aggs(i)}.retract(
             |      ((${accTypes(i)}) accs.getField($i)),
             |      ${parameters(i)});""".stripMargin
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
           |  public final org.apache.flink.types.Row createAccumulators()
           |    """.stripMargin
      val init: String =
        j"""
           |      org.apache.flink.types.Row accs =
           |          new org.apache.flink.types.Row(${aggs.length});"""
        .stripMargin
      val create: String = {
        for (i <- aggs.indices) yield
          j"""
             |    accs.setField(
             |      $i,
             |      ${aggs(i)}.createAccumulator());"""
          .stripMargin
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

    def genSetConstantFlags: String = {

      val sig: String =
        j"""
           |  public final void setConstantFlags(org.apache.flink.types.Row output)
           |    """.stripMargin

      val setFlags: String = if (constantFlags.isDefined) {
        {
          for (cf <- constantFlags.get) yield {
            j"""
               |    output.setField(${cf._1}, ${if (cf._2) "true" else "false"});"""
            .stripMargin
          }
        }.mkString("\n")
      } else {
        ""
      }

      j"""$sig {
         |$setFlags
         |  }""".stripMargin
    }

    def genCreateOutputRow: String = {
      j"""
         |  public final org.apache.flink.types.Row createOutputRow() {
         |    return new org.apache.flink.types.Row($outputArity);
         |  }""".stripMargin
    }

    def genMergeAccumulatorsPair: String = {

      val mapping = mergeMapping.getOrElse(aggs.indices.toArray)

      val sig: String =
        j"""
           |  public final org.apache.flink.types.Row mergeAccumulatorsPair(
           |    org.apache.flink.types.Row a,
           |    org.apache.flink.types.Row b)
           """.stripMargin
      val merge: String = {
        for (i <- aggs.indices) yield
          j"""
             |    ${accTypes(i)} aAcc$i = (${accTypes(i)}) a.getField($i);
             |    ${accTypes(i)} bAcc$i = (${accTypes(i)}) b.getField(${mapping(i)});
             |    accIt$i.setElement(bAcc$i);
             |    ${aggs(i)}.merge(aAcc$i, accIt$i);
             |    a.setField($i, aAcc$i);
          """.stripMargin
      }.mkString("\n")
      val ret: String =
        j"""
           |      return a;
           """.stripMargin

      if (needMerge) {
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
           |    org.apache.flink.types.Row accs)""".stripMargin

      val reset: String = {
        for (i <- aggs.indices) yield
          j"""
             |    ${aggs(i)}.resetAccumulator(
             |      ((${accTypes(i)}) accs.getField($i)));""".stripMargin
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

    val generatedAggregationsClass = classOf[GeneratedAggregations].getCanonicalName
    var funcCode =
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
         """.stripMargin

    funcCode += genSetAggregationResults + "\n"
    funcCode += genAccumulate + "\n"
    funcCode += genRetract + "\n"
    funcCode += genCreateAccumulators + "\n"
    funcCode += genSetForwardedFields + "\n"
    funcCode += genSetConstantFlags + "\n"
    funcCode += genCreateOutputRow + "\n"
    funcCode += genMergeAccumulatorsPair + "\n"
    funcCode += genResetAccumulator + "\n"
    funcCode += "}"

    GeneratedAggregationsFunction(funcName, funcCode)
  }

}
