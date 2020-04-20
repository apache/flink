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

package org.apache.flink.table.types.extraction

import java.util.Optional

import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.types.extraction.TypeInferenceExtractorTest.TestSpec
import org.apache.flink.table.types.inference.{ArgumentTypeStrategy, InputTypeStrategies, TypeStrategies}
import org.hamcrest.CoreMatchers.equalTo
import org.junit.Assert.assertThat
import org.junit.rules.ExpectedException
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.junit.{Rule, Test}

import scala.annotation.meta.getter

/**
 * Scala tests for [[TypeInferenceExtractor]].
 */
@RunWith(classOf[Parameterized])
class TypeInferenceExtractorScalaTest(testSpec: TestSpec) {

  @(Rule @getter)
  var thrown: ExpectedException = ExpectedException.none

  @Test
  def testArgumentNames(): Unit = {
    if (testSpec.expectedArgumentNames != null) {
      assertThat(
        testSpec.typeInferenceExtraction.get.getNamedArguments,
        equalTo(Optional.of(testSpec.expectedArgumentNames)))
    }
  }

  @Test
  def testArgumentTypes(): Unit = {
    if (testSpec.expectedArgumentTypes != null) {
      assertThat(
        testSpec.typeInferenceExtraction.get.getTypedArguments,
        equalTo(Optional.of(testSpec.expectedArgumentTypes)))
    }
  }

  @Test
  def testOutputTypeStrategy(): Unit = {
    if (!testSpec.expectedOutputStrategies.isEmpty) {
      assertThat(
        testSpec.typeInferenceExtraction.get.getOutputTypeStrategy,
        equalTo(TypeStrategies.mapping(testSpec.expectedOutputStrategies)))
    }
  }
}

object TypeInferenceExtractorScalaTest {

  @Parameters
  def testData: Array[TestSpec] = Array(

    // Scala function with data type hint
    TestSpec.forScalarFunction(classOf[ScalaScalarFunction])
      .expectNamedArguments("i", "s", "d")
      .expectTypedArguments(
        DataTypes.INT.notNull().bridgedTo(classOf[Int]),
        DataTypes.STRING,
        DataTypes.DECIMAL(10, 4))
      .expectOutputMapping(
        InputTypeStrategies.sequence(
          Array[String]("i", "s", "d"),
          Array[ArgumentTypeStrategy](
            InputTypeStrategies.explicit(DataTypes.INT.notNull().bridgedTo(classOf[Int])),
            InputTypeStrategies.explicit(DataTypes.STRING),
            InputTypeStrategies.explicit(DataTypes.DECIMAL(10, 4)))),
        TypeStrategies.explicit(DataTypes.BOOLEAN.notNull().bridgedTo(classOf[Boolean]))),

    // global output hint with local input overloading
    TestSpec.forScalarFunction(classOf[ScalaGlobalOutputFunctionHint])
      .expectOutputMapping(
        InputTypeStrategies.sequence(InputTypeStrategies.explicit(DataTypes.INT)),
        TypeStrategies.explicit(DataTypes.INT))
      .expectOutputMapping(
        InputTypeStrategies.sequence(InputTypeStrategies.explicit(DataTypes.STRING)),
        TypeStrategies.explicit(DataTypes.INT))
  )

  // ----------------------------------------------------------------------------------------------
  // Test classes for extraction
  // ----------------------------------------------------------------------------------------------

  private class ScalaScalarFunction extends ScalarFunction {
    def eval(
      i: Int,
      s: String,
      @DataTypeHint("DECIMAL(10, 4)") d: java.math.BigDecimal): Boolean = false
  }

  @FunctionHint(output = new DataTypeHint("INT"))
  private class ScalaGlobalOutputFunctionHint extends ScalarFunction {
    @FunctionHint(input = Array(new DataTypeHint("INT")))
    def eval(n: Integer): Integer = null

    @FunctionHint(input = Array(new DataTypeHint("STRING")))
    def eval(n: String): Integer = null
  }
}
