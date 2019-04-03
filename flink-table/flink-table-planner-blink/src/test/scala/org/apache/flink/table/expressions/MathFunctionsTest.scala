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
package org.apache.flink.table.expressions

import org.apache.flink.table.expressions.utils.ScalarTypesTestBase
import org.junit.{Ignore, Test}

class MathFunctionsTest extends ScalarTypesTestBase {

  // ----------------------------------------------------------------------------------------------
  // Math functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testMod(): Unit = {
    testSqlApi(
      "MOD(f4, f7)",
      "2")

    testSqlApi(
      "MOD(f4, 3)",
      "2")

    testSqlApi(
      "MOD(44, 3)",
      "2")
  }

  @Test
  def testExp(): Unit = {
    testSqlApi(
      "EXP(f2)",
      math.exp(42.toByte).toString)

    testSqlApi(
      "EXP(f3)",
      math.exp(43.toShort).toString)

    testSqlApi(
      "EXP(f4)",
      math.exp(44.toLong).toString)

    testSqlApi(
      "EXP(f5)",
      math.exp(4.5.toFloat).toString)

    testSqlApi(
      "EXP(f6)",
      math.exp(4.6).toString)

    testSqlApi(
      "EXP(f7)",
      math.exp(3).toString)

    testSqlApi(
      "EXP(3)",
      math.exp(3).toString)
  }

  @Test
  def testLog10(): Unit = {
    testSqlApi(
      "LOG10(f2)",
      math.log10(42.toByte).toString)

    testSqlApi(
      "LOG10(f3)",
      math.log10(43.toShort).toString)

    testSqlApi(
      "LOG10(f4)",
      math.log10(44.toLong).toString)

    testSqlApi(
      "LOG10(f5)",
      math.log10(4.5.toFloat).toString)

    testSqlApi(
      "LOG10(f6)",
      math.log10(4.6).toString)

    testSqlApi(
      "LOG10(f32)",
      math.log10(-1).toString)

    testSqlApi(
      "LOG10(f27)",
      math.log10(0).toString)
  }

  @Test
  def testPower(): Unit = {
    // f7: int , f4: long, f6: double
    testSqlApi(
      "POWER(f2, f7)",
      math.pow(42.toByte, 3).toString)

    testSqlApi(
      "POWER(f3, f6)",
      math.pow(43.toShort, 4.6D).toString)

    testSqlApi(
      "POWER(f4, f5)",
      math.pow(44.toLong, 4.5.toFloat).toString)

    testSqlApi(
      "POWER(f4, f5)",
      math.pow(44.toLong, 4.5.toFloat).toString)

    // f5: float
    testSqlApi(
      "power(f5, f5)",
      math.pow(4.5F, 4.5F).toString)

    testSqlApi(
      "power(f5, f6)",
      math.pow(4.5F, 4.6D).toString)

    testSqlApi(
      "power(f5, f7)",
      math.pow(4.5F, 3).toString)

    testSqlApi(
      "power(f5, f4)",
      math.pow(4.5F, 44L).toString)

    // f22: bigDecimal
    // TODO delete casting in SQL when CALCITE-1467 is fixed
    testSqlApi(
      "power(CAST(f22 AS DOUBLE), f5)",
      math.pow(2, 4.5F).toString)

    testSqlApi(
      "power(CAST(f22 AS DOUBLE), f6)",
      math.pow(2, 4.6D).toString)

    testSqlApi(
      "power(CAST(f22 AS DOUBLE), f7)",
      math.pow(2, 3).toString)

    testSqlApi(
      "power(CAST(f22 AS DOUBLE), f4)",
      math.pow(2, 44L).toString)

    testSqlApi(
      "power(f6, f22)",
      math.pow(4.6D, 2).toString)
  }

  @Test
  def testSqrt(): Unit = {
    testSqlApi(
      "SQRT(f6)",
      math.sqrt(4.6D).toString)

    testSqlApi(
      "SQRT(f7)",
      math.sqrt(3).toString)

    testSqlApi(
      "SQRT(f4)",
      math.sqrt(44L).toString)

    testSqlApi(
      "SQRT(CAST(f22 AS DOUBLE))",
      math.sqrt(2.0).toString)

    testSqlApi(
      "SQRT(f5)",
      math.pow(4.5F, 0.5).toString)

    testSqlApi(
      "SQRT(25)",
      "5.0")

    testSqlApi(
      "POWER(CAST(2.2 AS DOUBLE), CAST(0.5 AS DOUBLE))", // TODO fix FLINK-4621
      math.sqrt(2.2).toString)
  }

  @Test
  def testLn(): Unit = {
    testSqlApi(
      "LN(f2)",
      math.log(42.toByte).toString)

    testSqlApi(
      "LN(f3)",
      math.log(43.toShort).toString)

    testSqlApi(
      "LN(f4)",
      math.log(44.toLong).toString)

    testSqlApi(
      "LN(f5)",
      math.log(4.5.toFloat).toString)

    testSqlApi(
      "LN(f6)",
      math.log(4.6).toString)

    testSqlApi(
      "LN(f32)",
      math.log(-1).toString)

    testSqlApi(
      "LN(f27)",
      math.log(0).toString)
  }

  @Test
  def testAbs(): Unit = {
    testSqlApi(
      "ABS(f2)",
      "42")

    testSqlApi(
      "ABS(f3)",
      "43")

    testSqlApi(
      "ABS(f4)",
      "44")

    testSqlApi(
      "ABS(f5)",
      "4.5")

    testSqlApi(
      "ABS(f6)",
      "4.6")

    testSqlApi(
      "ABS(f9)",
      "42")

    testSqlApi(
      "ABS(f10)",
      "43")

    testSqlApi(
      "ABS(f11)",
      "44")

    testSqlApi(
      "ABS(f12)",
      "4.5")

    testSqlApi(
      "ABS(f13)",
      "4.6")

    testSqlApi(
      "ABS(f15)",
      "1231.1231231321321321111")
  }

  @Test
  def testArithmeticFloorCeil(): Unit = {
    testSqlApi(
      "FLOOR(f5)",
      "4.0")

    testSqlApi(
      "CEIL(f5)",
      "5.0")

    testSqlApi(
      "FLOOR(f3)",
      "43")

    testSqlApi(
      "CEIL(f3)",
      "43")

    testSqlApi(
      "FLOOR(f15)",
      "-1232")

    testSqlApi(
      "CEIL(f15)",
      "-1231")
  }

  @Test
  def testSin(): Unit = {
    testSqlApi(
      "SIN(f2)",
      math.sin(42.toByte).toString)

    testSqlApi(
      "SIN(f3)",
      math.sin(43.toShort).toString)

    testSqlApi(
      "SIN(f4)",
      math.sin(44.toLong).toString)

    testSqlApi(
      "SIN(f5)",
      math.sin(4.5.toFloat).toString)

    testSqlApi(
      "SIN(f6)",
      math.sin(4.6).toString)

    testSqlApi(
      "SIN(f15)",
      math.sin(-1231.1231231321321321111).toString)
  }

  @Test
  def testCos(): Unit = {
    testSqlApi(
      "COS(f2)",
      math.cos(42.toByte).toString)

    testSqlApi(
      "COS(f3)",
      math.cos(43.toShort).toString)

    testSqlApi(
      "COS(f4)",
      math.cos(44.toLong).toString)

    testSqlApi(
      "COS(f5)",
      math.cos(4.5.toFloat).toString)

    testSqlApi(
      "COS(f6)",
      math.cos(4.6).toString)

    testSqlApi(
      "COS(f15)",
      math.cos(-1231.1231231321321321111).toString)
  }

  @Test
  def testTan(): Unit = {
    testSqlApi(
      "TAN(f2)",
      math.tan(42.toByte).toString)

    testSqlApi(
      "TAN(f3)",
      math.tan(43.toShort).toString)

    testSqlApi(
      "TAN(f4)",
      math.tan(44.toLong).toString)

    testSqlApi(
      "TAN(f5)",
      math.tan(4.5.toFloat).toString)

    testSqlApi(
      "TAN(f6)",
      math.tan(4.6).toString)

    testSqlApi(
      "TAN(f15)",
      math.tan(-1231.1231231321321321111).toString)
  }

  @Test
  def testCot(): Unit = {
    testSqlApi(
      "COT(f2)",
      (1.0d / math.tan(42.toByte)).toString)

    testSqlApi(
      "COT(f3)",
      (1.0d / math.tan(43.toShort)).toString)

    testSqlApi(
      "COT(f4)",
      (1.0d / math.tan(44.toLong)).toString)

    testSqlApi(
      "COT(f5)",
      (1.0d / math.tan(4.5.toFloat)).toString)

    testSqlApi(
      "COT(f6)",
      (1.0d / math.tan(4.6)).toString)

    testSqlApi(
      "COT(f15)",
      (1.0d / math.tan(-1231.1231231321321321111)).toString)
  }

  @Test
  def testAsin(): Unit = {
    testSqlApi(
      "ASIN(f25)",
      math.asin(0.42.toByte).toString)

    testSqlApi(
      "ASIN(f26)",
      math.asin(0.toShort).toString)

    testSqlApi(
      "ASIN(f27)",
      math.asin(0.toLong).toString)

    testSqlApi(
      "ASIN(f28)",
      math.asin(0.45.toFloat).toString)

    testSqlApi(
      "ASIN(f29)",
      math.asin(0.46).toString)

    testSqlApi(
      "ASIN(f30)",
      math.asin(1).toString)

    testSqlApi(
      "ASIN(f31)",
      math.asin(-0.1231231321321321111).toString)
  }

  @Test
  def testAcos(): Unit = {
    testSqlApi(
      "ACOS(f25)",
      math.acos(0.42.toByte).toString)

    testSqlApi(
      "ACOS(f26)",
      math.acos(0.toShort).toString)

    testSqlApi(
      "ACOS(f27)",
      math.acos(0.toLong).toString)

    testSqlApi(
      "ACOS(f28)",
      math.acos(0.45.toFloat).toString)

    testSqlApi(
      "ACOS(f29)",
      math.acos(0.46).toString)

    testSqlApi(
      "ACOS(f30)",
      math.acos(1).toString)

    testSqlApi(
      "ACOS(f31)",
      math.acos(-0.1231231321321321111).toString)
  }

  @Test
  def testAtan(): Unit = {
    testSqlApi(
      "ATAN(f25)",
      math.atan(0.42.toByte).toString)

    testSqlApi(
      "ATAN(f26)",
      math.atan(0.toShort).toString)

    testSqlApi(
      "ATAN(f27)",
      math.atan(0.toLong).toString)

    testSqlApi(
      "ATAN(f28)",
      math.atan(0.45.toFloat).toString)

    testSqlApi(
      "ATAN(f29)",
      math.atan(0.46).toString)

    testSqlApi(
      "ATAN(f30)",
      math.atan(1).toString)

    testSqlApi(
      "ATAN(f31)",
      math.atan(-0.1231231321321321111).toString)
  }

  @Test
  def testDegrees(): Unit = {
    testSqlApi(
      "DEGREES(f2)",
      math.toDegrees(42.toByte).toString)

    testSqlApi(
      "DEGREES(f3)",
      math.toDegrees(43.toShort).toString)

    testSqlApi(
      "DEGREES(f4)",
      math.toDegrees(44.toLong).toString)

    testSqlApi(
      "DEGREES(f5)",
      math.toDegrees(4.5.toFloat).toString)

    testSqlApi(
      "DEGREES(f6)",
      math.toDegrees(4.6).toString)

    testSqlApi(
      "DEGREES(f15)",
      math.toDegrees(-1231.1231231321321321111).toString)
  }

  @Test
  def testRadians(): Unit = {
    testSqlApi(
      "RADIANS(f2)",
      math.toRadians(42.toByte).toString)

    testSqlApi(
      "RADIANS(f3)",
      math.toRadians(43.toShort).toString)

    testSqlApi(
      "RADIANS(f4)",
      math.toRadians(44.toLong).toString)

    testSqlApi(
      "RADIANS(f5)",
      math.toRadians(4.5.toFloat).toString)

    testSqlApi(
      "RADIANS(f6)",
      math.toRadians(4.6).toString)

    testSqlApi(
      "RADIANS(f15)",
      math.toRadians(-1231.1231231321321321111).toString)
  }

  @Test
  def testSign(): Unit = {
    testSqlApi(
      "SIGN(f4)",
      1.toString)

    testSqlApi(
      "SIGN(f6)",
      1.0.toString)

    testSqlApi(
      "SIGN(f15)",
      "-1.0000000000000000000") // calcite: SIGN(Decimal(p,s)) => Decimal(p,s)
  }

  @Test
  def testRound(): Unit = {
    testSqlApi(
      "ROUND(f29, f30)",
      0.5.toString)

    testSqlApi(
      "ROUND(f31, f7)",
      "-0.123")

    testSqlApi(
      "ROUND(f4, f32)",
      40.toString)
  }

  @Test
  def testPi(): Unit = {
    testSqlApi(
      "pi()",
      math.Pi.toString)
  }

  @Test
  def testRandAndRandInteger(): Unit = {
    val random1 = new java.util.Random(1)
    testSqlApi(
      "RAND(1)",
      random1.nextDouble().toString)

    val random2 = new java.util.Random(3)
    testSqlApi(
      "RAND(f7)",
      random2.nextDouble().toString)

    val random3 = new java.util.Random(1)
    testSqlApi(
      "RAND_INTEGER(1, 10)",
      random3.nextInt(10).toString)

    val random4 = new java.util.Random(3)
    testSqlApi(
      "RAND_INTEGER(f7, CAST(f4 AS INT))",
      random4.nextInt(44).toString)
  }

  @Test
  def testE(): Unit = {
    testSqlApi(
      "E()",
      math.E.toString)

    testSqlApi(
      "e()",
      math.E.toString)
  }

  @Test
  def testLog(): Unit = {
    testSqlApi(
      "LOG(f6)",
      "1.5260563034950492"
    )

    testSqlApi(
      "LOG(f6-f6 + 10, f6-f6+100)",
      "2.0"
    )

    testSqlApi(
      "LOG(f6+20)",
      "3.202746442938317"
    )

    testSqlApi(
      "LOG(10)",
      "2.302585092994046"
    )

    testSqlApi(
      "LOG(10, 100)",
      "2.0"
    )

    testSqlApi(
      "log(f32, f32)",
      (math.log(-1)/math.log(-1)).toString)

    testSqlApi(
      "log(f27, f32)",
      (math.log(0)/math.log(0)).toString)
  }

  @Test
  def testLog2(): Unit = {
    testSqlApi(
      "log2(f2)",
      (math.log(42.toByte)/math.log(2.toByte)).toString)

    testSqlApi(
      "log2(f3)",
      (math.log(43.toShort)/math.log(2.toShort)).toString)

    testSqlApi(
      "log2(f4)",
      (math.log(44.toLong)/math.log(2.toLong)).toString)

    testSqlApi(
      "log2(f5)",
      (math.log(4.5.toFloat)/math.log(2.toFloat)).toString)

    testSqlApi(
      "log2(f6)",
      (math.log(4.6)/math.log(2)).toString)

    testSqlApi(
      "log2(f32)",
      (math.log(-1)/math.log(2)).toString)

    testSqlApi(
      "log2(f27)",
      (math.log(0)/math.log(2)).toString)
  }
}
