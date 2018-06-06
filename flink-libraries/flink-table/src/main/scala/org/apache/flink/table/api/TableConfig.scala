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
package org.apache.flink.table.api

import _root_.java.util.TimeZone
import _root_.java.math.MathContext

import org.apache.flink.table.calcite.CalciteConfig

/**
 * A config to define the runtime behavior of the Table API.
 */
class TableConfig {

  /**
   * Defines the timezone for date/time/timestamp conversions.
   */
  private var timeZone: TimeZone = TimeZone.getTimeZone("UTC")

  /**
   * Defines if all fields need to be checked for NULL first.
   */
  private var nullCheck: Boolean = true

  /**
    * Defines the configuration of Calcite for Table API and SQL queries.
    */
  private var calciteConfig: CalciteConfig = CalciteConfig.DEFAULT

  /**
    * Defines the default context for decimal division calculation.
    * We use Scala's default MathContext.DECIMAL128.
    */
  private var decimalContext: MathContext = MathContext.DECIMAL128

  /**
    * Specifies a threshold where generated code will be split into sub-function calls. Java has a
    * maximum method length of 64 KB. This setting allows for finer granularity if necessary.
    */
  private var maxGeneratedCodeLength: Int = 64000 // just an estimate

  /**
   * Sets the timezone for date/time/timestamp conversions.
   */
  def setTimeZone(timeZone: TimeZone): Unit = {
    require(timeZone != null, "timeZone must not be null.")
    this.timeZone = timeZone
  }

  /**
   * Returns the timezone for date/time/timestamp conversions.
   */
  def getTimeZone: TimeZone = timeZone

  /**
   * Returns the NULL check. If enabled, all fields need to be checked for NULL first.
   */
  def getNullCheck: Boolean = nullCheck

  /**
   * Sets the NULL check. If enabled, all fields need to be checked for NULL first.
   */
  def setNullCheck(nullCheck: Boolean): Unit = {
    this.nullCheck = nullCheck
  }

  /**
    * Returns the current configuration of Calcite for Table API and SQL queries.
    */
  def getCalciteConfig: CalciteConfig = calciteConfig

  /**
    * Sets the configuration of Calcite for Table API and SQL queries.
    * Changing the configuration has no effect after the first query has been defined.
    */
  def setCalciteConfig(calciteConfig: CalciteConfig): Unit = {
    this.calciteConfig = calciteConfig
  }

  /**
    * Returns the default context for decimal division calculation.
    * [[_root_.java.math.MathContext#DECIMAL128]] by default.
    */
  def getDecimalContext: MathContext = decimalContext

  /**
    * Sets the default context for decimal division calculation.
    * [[_root_.java.math.MathContext#DECIMAL128]] by default.
    */
  def setDecimalContext(mathContext: MathContext): Unit = {
    this.decimalContext = mathContext
  }

  /**
    * Returns the current threshold where generated code will be split into sub-function calls.
    * Java has a maximum method length of 64 KB. This setting allows for finer granularity if
    * necessary. Default is 64000.
    */
  def getMaxGeneratedCodeLength: Int = maxGeneratedCodeLength

  /**
    * Returns the current threshold where generated code will be split into sub-function calls.
    * Java has a maximum method length of 64 KB. This setting allows for finer granularity if
    * necessary. Default is 64000.
    */
  def setMaxGeneratedCodeLength(maxGeneratedCodeLength: Int): Unit = {
    if (maxGeneratedCodeLength <= 0) {
      throw new IllegalArgumentException("Length must be greater than 0.")
    }
    this.maxGeneratedCodeLength = maxGeneratedCodeLength
  }
}

object TableConfig {
  def DEFAULT = new TableConfig()
}
