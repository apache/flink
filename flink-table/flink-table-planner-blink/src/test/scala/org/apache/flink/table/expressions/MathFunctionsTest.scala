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

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala.{e, pi, rand, randInteger, _}
import org.apache.flink.table.expressions.utils.ScalarTypesTestBase

import org.junit.{Ignore, Test}

class MathFunctionsTest extends ScalarTypesTestBase {

  // ----------------------------------------------------------------------------------------------
  // Math functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testMod(): Unit = {
    testAllApis(
      'f4.mod('f7),
      "f4.mod(f7)",
      "MOD(f4, f7)",
      "2")

    testAllApis(
      'f4.mod(3),
      "mod(f4, 3)",
      "MOD(f4, 3)",
      "2")

    testAllApis(
      'f4 % 3,
      "mod(44, 3)",
      "MOD(44, 3)",
      "2")
  }

  @Test
  def testExp(): Unit = {
    testAllApis(
      'f2.exp(),
      "f2.exp()",
      "EXP(f2)",
      math.exp(42.toByte).toString)

    testAllApis(
      'f3.exp(),
      "f3.exp()",
      "EXP(f3)",
      math.exp(43.toShort).toString)

    testAllApis(
      'f4.exp(),
      "f4.exp()",
      "EXP(f4)",
      math.exp(44.toLong).toString)

    testAllApis(
      'f5.exp(),
      "f5.exp()",
      "EXP(f5)",
      math.exp(4.5.toFloat).toString)

    testAllApis(
      'f6.exp(),
      "f6.exp()",
      "EXP(f6)",
      math.exp(4.6).toString)

    testAllApis(
      'f7.exp(),
      "exp(f7)",
      "EXP(f7)",
      math.exp(3).toString)

    testAllApis(
      3.exp(),
      "exp(3)",
      "EXP(3)",
      math.exp(3).toString)
  }

  @Test
  def testLog10(): Unit = {
    testAllApis(
      'f2.log10(),
      "f2.log10()",
      "LOG10(f2)",
      math.log10(42.toByte).toString)

    testAllApis(
      'f3.log10(),
      "f3.log10()",
      "LOG10(f3)",
      math.log10(43.toShort).toString)

    testAllApis(
      'f4.log10(),
      "f4.log10()",
      "LOG10(f4)",
      math.log10(44.toLong).toString)

    testAllApis(
      'f5.log10(),
      "f5.log10()",
      "LOG10(f5)",
      math.log10(4.5.toFloat).toString)

    testAllApis(
      'f6.log10(),
      "f6.log10()",
      "LOG10(f6)",
      math.log10(4.6).toString)

    testAllApis(
      'f32.log10(),
      "f32.log10()",
      "LOG10(f32)",
      math.log10(-1).toString)

    testAllApis(
      'f27.log10(),
      "f27.log10()",
      "LOG10(f27)",
      math.log10(0).toString)
  }

  @Test
  def testPower(): Unit = {
    // f7: int , f4: long, f6: double
    testAllApis(
      'f2.power('f7),
      "f2.power(f7)",
      "POWER(f2, f7)",
      math.pow(42.toByte, 3).toString)

    testAllApis(
      'f3.power('f6),
      "f3.power(f6)",
      "POWER(f3, f6)",
      math.pow(43.toShort, 4.6D).toString)

    testAllApis(
      'f4.power('f5),
      "f4.power(f5)",
      "POWER(f4, f5)",
      math.pow(44.toLong, 4.5.toFloat).toString)

    testAllApis(
      'f4.power('f5),
      "f4.power(f5)",
      "POWER(f4, f5)",
      math.pow(44.toLong, 4.5.toFloat).toString)

    // f5: float
    testAllApis('f5.power('f5),
      "f5.power(f5)",
      "power(f5, f5)",
      math.pow(4.5F, 4.5F).toString)

    testAllApis('f5.power('f6),
      "f5.power(f6)",
      "power(f5, f6)",
      math.pow(4.5F, 4.6D).toString)

    testAllApis('f5.power('f7),
      "f5.power(f7)",
      "power(f5, f7)",
      math.pow(4.5F, 3).toString)

    testAllApis('f5.power('f4),
      "f5.power(f4)",
      "power(f5, f4)",
      math.pow(4.5F, 44L).toString)

    // f22: bigDecimal
    // TODO delete casting in SQL when CALCITE-1467 is fixed
    testAllApis(
      'f22.cast(DataTypes.DOUBLE).power('f5),
      "f22.cast(DOUBLE).power(f5)",
      "power(CAST(f22 AS DOUBLE), f5)",
      math.pow(2, 4.5F).toString)

    testAllApis(
      'f22.cast(DataTypes.DOUBLE).power('f6),
      "f22.cast(DOUBLE).power(f6)",
      "power(CAST(f22 AS DOUBLE), f6)",
      math.pow(2, 4.6D).toString)

    testAllApis(
      'f22.cast(DataTypes.DOUBLE).power('f7),
      "f22.cast(DOUBLE).power(f7)",
      "power(CAST(f22 AS DOUBLE), f7)",
      math.pow(2, 3).toString)

    testAllApis(
      'f22.cast(DataTypes.DOUBLE).power('f4),
      "f22.cast(DOUBLE).power(f4)",
      "power(CAST(f22 AS DOUBLE), f4)",
      math.pow(2, 44L).toString)

    testAllApis(
      'f6.power('f22.cast(DataTypes.DOUBLE)),
      "f6.power(f22.cast(DOUBLE))",
      "power(f6, f22)",
      math.pow(4.6D, 2).toString)
  }

  @Ignore // TODO
  @Test
  def testSqrt(): Unit = {
    testAllApis(
      'f6.sqrt(),
      "f6.sqrt",
      "SQRT(f6)",
      math.sqrt(4.6D).toString)

    testAllApis(
      'f7.sqrt(),
      "f7.sqrt",
      "SQRT(f7)",
      math.sqrt(3).toString)

    testAllApis(
      'f4.sqrt(),
      "f4.sqrt",
      "SQRT(f4)",
      math.sqrt(44L).toString)

    testAllApis(
      'f22.cast(DataTypes.DOUBLE).sqrt(),
      "f22.cast(DOUBLE).sqrt",
      "SQRT(CAST(f22 AS DOUBLE))",
      math.sqrt(2.0).toString)

    testAllApis(
      'f5.sqrt(),
      "f5.sqrt",
      "SQRT(f5)",
      math.pow(4.5F, 0.5).toString)

    testAllApis(
      25.sqrt(),
      "25.sqrt()",
      "SQRT(25)",
      "5.0")

    testAllApis(
      2.2.sqrt(),
      "2.2.sqrt()",
      "POWER(CAST(2.2 AS DOUBLE), CAST(0.5 AS DOUBLE))", // TODO fix FLINK-4621
      math.sqrt(2.2).toString)
  }

  @Test
  def testLn(): Unit = {
    testAllApis(
      'f2.ln(),
      "f2.ln()",
      "LN(f2)",
      math.log(42.toByte).toString)

    testAllApis(
      'f3.ln(),
      "f3.ln()",
      "LN(f3)",
      math.log(43.toShort).toString)

    testAllApis(
      'f4.ln(),
      "f4.ln()",
      "LN(f4)",
      math.log(44.toLong).toString)

    testAllApis(
      'f5.ln(),
      "f5.ln()",
      "LN(f5)",
      math.log(4.5.toFloat).toString)

    testAllApis(
      'f6.ln(),
      "f6.ln()",
      "LN(f6)",
      math.log(4.6).toString)

    testAllApis(
      'f32.ln(),
      "f32.ln()",
      "LN(f32)",
      math.log(-1).toString)

    testAllApis(
      'f27.ln(),
      "f27.ln()",
      "LN(f27)",
      math.log(0).toString)
  }

  @Test
  def testAbs(): Unit = {
    testAllApis(
      'f2.abs(),
      "f2.abs()",
      "ABS(f2)",
      "42")

    testAllApis(
      'f3.abs(),
      "f3.abs()",
      "ABS(f3)",
      "43")

    testAllApis(
      'f4.abs(),
      "f4.abs()",
      "ABS(f4)",
      "44")

    testAllApis(
      'f5.abs(),
      "f5.abs()",
      "ABS(f5)",
      "4.5")

    testAllApis(
      'f6.abs(),
      "f6.abs()",
      "ABS(f6)",
      "4.6")

    testAllApis(
      'f9.abs(),
      "f9.abs()",
      "ABS(f9)",
      "42")

    testAllApis(
      'f10.abs(),
      "f10.abs()",
      "ABS(f10)",
      "43")

    testAllApis(
      'f11.abs(),
      "f11.abs()",
      "ABS(f11)",
      "44")

    testAllApis(
      'f12.abs(),
      "f12.abs()",
      "ABS(f12)",
      "4.5")

    testAllApis(
      'f13.abs(),
      "f13.abs()",
      "ABS(f13)",
      "4.6")

    testAllApis(
      'f15.abs(),
      "f15.abs()",
      "ABS(f15)",
      "1231.1231231321321321111")
  }

  @Test
  def testArithmeticFloorCeil(): Unit = {
    testAllApis(
      'f5.floor(),
      "f5.floor()",
      "FLOOR(f5)",
      "4.0")

    testAllApis(
      'f5.ceil(),
      "f5.ceil()",
      "CEIL(f5)",
      "5.0")

    testAllApis(
      'f3.floor(),
      "f3.floor()",
      "FLOOR(f3)",
      "43")

    testAllApis(
      'f3.ceil(),
      "f3.ceil()",
      "CEIL(f3)",
      "43")

    testAllApis(
      'f15.floor(),
      "f15.floor()",
      "FLOOR(f15)",
      "-1232")

    testAllApis(
      'f15.ceil(),
      "f15.ceil()",
      "CEIL(f15)",
      "-1231")
  }

  @Test
  def testSin(): Unit = {
    testAllApis(
      'f2.sin(),
      "f2.sin()",
      "SIN(f2)",
      math.sin(42.toByte).toString)

    testAllApis(
      'f3.sin(),
      "f3.sin()",
      "SIN(f3)",
      math.sin(43.toShort).toString)

    testAllApis(
      'f4.sin(),
      "f4.sin()",
      "SIN(f4)",
      math.sin(44.toLong).toString)

    testAllApis(
      'f5.sin(),
      "f5.sin()",
      "SIN(f5)",
      math.sin(4.5.toFloat).toString)

    testAllApis(
      'f6.sin(),
      "f6.sin()",
      "SIN(f6)",
      math.sin(4.6).toString)

    testAllApis(
      'f15.sin(),
      "sin(f15)",
      "SIN(f15)",
      math.sin(-1231.1231231321321321111).toString)
  }

  @Test
  def testCos(): Unit = {
    testAllApis(
      'f2.cos(),
      "f2.cos()",
      "COS(f2)",
      math.cos(42.toByte).toString)

    testAllApis(
      'f3.cos(),
      "f3.cos()",
      "COS(f3)",
      math.cos(43.toShort).toString)

    testAllApis(
      'f4.cos(),
      "f4.cos()",
      "COS(f4)",
      math.cos(44.toLong).toString)

    testAllApis(
      'f5.cos(),
      "f5.cos()",
      "COS(f5)",
      math.cos(4.5.toFloat).toString)

    testAllApis(
      'f6.cos(),
      "f6.cos()",
      "COS(f6)",
      math.cos(4.6).toString)

    testAllApis(
      'f15.cos(),
      "cos(f15)",
      "COS(f15)",
      math.cos(-1231.1231231321321321111).toString)
  }

  @Test
  def testTan(): Unit = {
    testAllApis(
      'f2.tan(),
      "f2.tan()",
      "TAN(f2)",
      math.tan(42.toByte).toString)

    testAllApis(
      'f3.tan(),
      "f3.tan()",
      "TAN(f3)",
      math.tan(43.toShort).toString)

    testAllApis(
      'f4.tan(),
      "f4.tan()",
      "TAN(f4)",
      math.tan(44.toLong).toString)

    testAllApis(
      'f5.tan(),
      "f5.tan()",
      "TAN(f5)",
      math.tan(4.5.toFloat).toString)

    testAllApis(
      'f6.tan(),
      "f6.tan()",
      "TAN(f6)",
      math.tan(4.6).toString)

    testAllApis(
      'f15.tan(),
      "tan(f15)",
      "TAN(f15)",
      math.tan(-1231.1231231321321321111).toString)
  }

  @Test
  def testCot(): Unit = {
    testAllApis(
      'f2.cot(),
      "f2.cot()",
      "COT(f2)",
      (1.0d / math.tan(42.toByte)).toString)

    testAllApis(
      'f3.cot(),
      "f3.cot()",
      "COT(f3)",
      (1.0d / math.tan(43.toShort)).toString)

    testAllApis(
      'f4.cot(),
      "f4.cot()",
      "COT(f4)",
      (1.0d / math.tan(44.toLong)).toString)

    testAllApis(
      'f5.cot(),
      "f5.cot()",
      "COT(f5)",
      (1.0d / math.tan(4.5.toFloat)).toString)

    testAllApis(
      'f6.cot(),
      "f6.cot()",
      "COT(f6)",
      (1.0d / math.tan(4.6)).toString)

    testAllApis(
      'f15.cot(),
      "cot(f15)",
      "COT(f15)",
      (1.0d / math.tan(-1231.1231231321321321111)).toString)
  }

  @Test
  def testAsin(): Unit = {
    testAllApis(
      'f25.asin(),
      "f25.asin()",
      "ASIN(f25)",
      math.asin(0.42.toByte).toString)

    testAllApis(
      'f26.asin(),
      "f26.asin()",
      "ASIN(f26)",
      math.asin(0.toShort).toString)

    testAllApis(
      'f27.asin(),
      "f27.asin()",
      "ASIN(f27)",
      math.asin(0.toLong).toString)

    testAllApis(
      'f28.asin(),
      "f28.asin()",
      "ASIN(f28)",
      math.asin(0.45.toFloat).toString)

    testAllApis(
      'f29.asin(),
      "f29.asin()",
      "ASIN(f29)",
      math.asin(0.46).toString)

    testAllApis(
      'f30.asin(),
      "f30.asin()",
      "ASIN(f30)",
      math.asin(1).toString)

    testAllApis(
      'f31.asin(),
      "f31.asin()",
      "ASIN(f31)",
      math.asin(-0.1231231321321321111).toString)
  }

  @Test
  def testAcos(): Unit = {
    testAllApis(
      'f25.acos(),
      "f25.acos()",
      "ACOS(f25)",
      math.acos(0.42.toByte).toString)

    testAllApis(
      'f26.acos(),
      "f26.acos()",
      "ACOS(f26)",
      math.acos(0.toShort).toString)

    testAllApis(
      'f27.acos(),
      "f27.acos()",
      "ACOS(f27)",
      math.acos(0.toLong).toString)

    testAllApis(
      'f28.acos(),
      "f28.acos()",
      "ACOS(f28)",
      math.acos(0.45.toFloat).toString)

    testAllApis(
      'f29.acos(),
      "f29.acos()",
      "ACOS(f29)",
      math.acos(0.46).toString)

    testAllApis(
      'f30.acos(),
      "f30.acos()",
      "ACOS(f30)",
      math.acos(1).toString)

    testAllApis(
      'f31.acos(),
      "f31.acos()",
      "ACOS(f31)",
      math.acos(-0.1231231321321321111).toString)
  }

  @Test
  def testAtan(): Unit = {
    testAllApis(
      'f25.atan(),
      "f25.atan()",
      "ATAN(f25)",
      math.atan(0.42.toByte).toString)

    testAllApis(
      'f26.atan(),
      "f26.atan()",
      "ATAN(f26)",
      math.atan(0.toShort).toString)

    testAllApis(
      'f27.atan(),
      "f27.atan()",
      "ATAN(f27)",
      math.atan(0.toLong).toString)

    testAllApis(
      'f28.atan(),
      "f28.atan()",
      "ATAN(f28)",
      math.atan(0.45.toFloat).toString)

    testAllApis(
      'f29.atan(),
      "f29.atan()",
      "ATAN(f29)",
      math.atan(0.46).toString)

    testAllApis(
      'f30.atan(),
      "f30.atan()",
      "ATAN(f30)",
      math.atan(1).toString)

    testAllApis(
      'f31.atan(),
      "f31.atan()",
      "ATAN(f31)",
      math.atan(-0.1231231321321321111).toString)
  }

  @Test
  def testDegrees(): Unit = {
    testAllApis(
      'f2.degrees(),
      "f2.degrees()",
      "DEGREES(f2)",
      math.toDegrees(42.toByte).toString)

    testAllApis(
      'f3.degrees(),
      "f3.degrees()",
      "DEGREES(f3)",
      math.toDegrees(43.toShort).toString)

    testAllApis(
      'f4.degrees(),
      "f4.degrees()",
      "DEGREES(f4)",
      math.toDegrees(44.toLong).toString)

    testAllApis(
      'f5.degrees(),
      "f5.degrees()",
      "DEGREES(f5)",
      math.toDegrees(4.5.toFloat).toString)

    testAllApis(
      'f6.degrees(),
      "f6.degrees()",
      "DEGREES(f6)",
      math.toDegrees(4.6).toString)

    testAllApis(
      'f15.degrees(),
      "degrees(f15)",
      "DEGREES(f15)",
      math.toDegrees(-1231.1231231321321321111).toString)
  }

  @Test
  def testRadians(): Unit = {
    testAllApis(
      'f2.radians(),
      "f2.radians()",
      "RADIANS(f2)",
      math.toRadians(42.toByte).toString)

    testAllApis(
      'f3.radians(),
      "f3.radians()",
      "RADIANS(f3)",
      math.toRadians(43.toShort).toString)

    testAllApis(
      'f4.radians(),
      "f4.radians()",
      "RADIANS(f4)",
      math.toRadians(44.toLong).toString)

    testAllApis(
      'f5.radians(),
      "f5.radians()",
      "RADIANS(f5)",
      math.toRadians(4.5.toFloat).toString)

    testAllApis(
      'f6.radians(),
      "f6.radians()",
      "RADIANS(f6)",
      math.toRadians(4.6).toString)

    testAllApis(
      'f15.radians(),
      "radians(f15)",
      "RADIANS(f15)",
      math.toRadians(-1231.1231231321321321111).toString)
  }

  @Test
  def testSign(): Unit = {
    testAllApis(
      'f4.sign(),
      "f4.sign()",
      "SIGN(f4)",
      1.toString)

    testAllApis(
      'f6.sign(),
      "f6.sign()",
      "SIGN(f6)",
      1.0.toString)

    testAllApis(
      'f15.sign(),
      "sign(f15)",
      "SIGN(f15)",
      "-1.0000000000000000000") // calcite: SIGN(Decimal(p,s)) => Decimal(p,s)
  }

  @Test
  def testRound(): Unit = {
    testAllApis(
      'f29.round('f30),
      "f29.round(f30)",
      "ROUND(f29, f30)",
      0.5.toString)

    testAllApis(
      'f31.round('f7),
      "f31.round(f7)",
      "ROUND(f31, f7)",
      "-0.123")

    testAllApis(
      'f4.round('f32),
      "f4.round(f32)",
      "ROUND(f4, f32)",
      40.toString)
  }

  @Test
  def testPi(): Unit = {
    testAllApis(
      pi(),
      "pi()",
      "pi()",
      math.Pi.toString)
  }

  @Test
  def testRandAndRandInteger(): Unit = {
    val random1 = new java.util.Random(1)
    testAllApis(
      rand(1),
      "rand(1)",
      "RAND(1)",
      random1.nextDouble().toString)

    val random2 = new java.util.Random(3)
    testAllApis(
      rand('f7),
      "rand(f7)",
      "RAND(f7)",
      random2.nextDouble().toString)

    val random3 = new java.util.Random(1)
    testAllApis(
      randInteger(1, 10),
      "randInteger(1, 10)",
      "RAND_INTEGER(1, 10)",
      random3.nextInt(10).toString)

    val random4 = new java.util.Random(3)
    testAllApis(
      randInteger('f7, 'f4.cast(DataTypes.INT)),
      "randInteger(f7, f4.cast(INT))",
      "RAND_INTEGER(f7, CAST(f4 AS INT))",
      random4.nextInt(44).toString)
  }

  @Test
  def testE(): Unit = {
    testAllApis(
      e(),
      "E()",
      "E()",
      math.E.toString)

    testAllApis(
      e(),
      "e()",
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
