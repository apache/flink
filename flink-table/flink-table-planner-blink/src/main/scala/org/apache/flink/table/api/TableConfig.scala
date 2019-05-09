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

import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.{Configuration, GlobalConfiguration}
import org.apache.flink.table.api.OperatorType.OperatorType
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.util.Preconditions

import _root_.java.math.MathContext
import _root_.java.util.TimeZone

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
    * Defines the default context for decimal division calculation.
    * We use Scala's default MathContext.DECIMAL128.
    */
  private var decimalContext: MathContext = MathContext.DECIMAL128

  /**
    * Specifies a threshold where generated code will be split into sub-function calls. Java has a
    * maximum method length of 64 KB. This setting allows for finer granularity if necessary.
    */
  private var maxGeneratedCodeLength: Int = 64000 // just an estimate

  private val DEFAULT_FIRE_INTERVAL = Long.MinValue

  /**
    * The early firing interval in milli second, early fire is the emit strategy
    * before watermark advanced to end of window.
    *
    * < 0 means no early fire
    * 0 means no delay (fire on every element).
    * > 0 means the fire interval
    */
  private var earlyFireInterval = DEFAULT_FIRE_INTERVAL

  /**
    * The late firing interval in milli second, late fire is the emit strategy
    * after watermark advanced to end of window.
    *
    * < 0 means no late fire, drop every late elements
    * 0 means no delay (fire on every element).
    * > 0 means the fire interval
    *
    * NOTE: late firing strategy is only enabled when allowLateness > 0
    */
  private var lateFireInterval = DEFAULT_FIRE_INTERVAL

  /**
    * Defines the configuration of Calcite for Table API and SQL queries.
    */
  private var calciteConfig = CalciteConfig.createBuilder().build()

  /**
    * Defines user-defined configuration
    */
  private var conf = GlobalConfiguration.loadConfiguration()

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

  /**
    * Returns user-defined configuration
    */
  def getConf: Configuration = conf

  /**
    * Sets user-defined configuration
    */
  def setConf(conf: Configuration): Unit = {
    this.conf = GlobalConfiguration.loadConfiguration()
    this.conf.addAll(conf)
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
    this.calciteConfig = Preconditions.checkNotNull(calciteConfig)
  }

  /**
    * Returns true if given [[OperatorType]] is enabled, else false.
    */
  def isOperatorEnabled(operator: OperatorType): Boolean = {
    val disableOperators = conf.getString(TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS)
      .split(",")
      .map(_.trim)
    if (disableOperators.contains("HashJoin") &&
      (operator == OperatorType.BroadcastHashJoin ||
        operator == OperatorType.ShuffleHashJoin)) {
      false
    } else {
      !disableOperators.contains(operator.toString)
    }
  }

  /**
    * Specifies a minimum and a maximum time interval for how long idle state, i.e., state which
    * was not updated, will be retained.
    * State will never be cleared until it was idle for less than the minimum time and will never
    * be kept if it was idle for more than the maximum time.
    *
    * When new data arrives for previously cleaned-up state, the new data will be handled as if it
    * was the first data. This can result in previous results being overwritten.
    *
    * Set to 0 (zero) to never clean-up the state.
    *
    * @param minTime The minimum time interval for which idle state is retained. Set to 0 (zero) to
    *                never clean-up the state.
    * @param maxTime The maximum time interval for which idle state is retained. May not be smaller
    *                than than minTime. Set to 0 (zero) to never clean-up the state.
    */
  def withIdleStateRetentionTime(minTime: Time, maxTime: Time): TableConfig = {
    if (maxTime.toMilliseconds < minTime.toMilliseconds) {
      throw new IllegalArgumentException("maxTime may not be smaller than minTime.")
    }
    this.conf.setLong(TableConfigOptions.SQL_EXEC_STATE_TTL_MS, minTime.toMilliseconds)
    this.conf.setLong(TableConfigOptions.SQL_EXEC_STATE_TTL_MAX_MS, maxTime.toMilliseconds)
    this
  }

  /**
    * Returns the minimum time until state which was not updated will be retained.
    */
  def getMinIdleStateRetentionTime: Long = {
    this.conf.getLong(TableConfigOptions.SQL_EXEC_STATE_TTL_MS)
  }

  /**
    * Returns the maximum time until state which was not updated will be retained.
    */
  def getMaxIdleStateRetentionTime: Long = {
    // only min idle ttl provided.
    if (this.conf.contains(TableConfigOptions.SQL_EXEC_STATE_TTL_MS)
      && !this.conf.contains(TableConfigOptions.SQL_EXEC_STATE_TTL_MAX_MS)) {
      this.conf.setLong(TableConfigOptions.SQL_EXEC_STATE_TTL_MAX_MS,
        getMinIdleStateRetentionTime * 2)
    }
    this.conf.getLong(TableConfigOptions.SQL_EXEC_STATE_TTL_MAX_MS)
  }

  /**
    * Specifies the early firing interval in milli second, early fire is the emit strategy
    * before watermark advanced to end of window.
    */
  def withEarlyFireInterval(interval: Time): TableConfig = {
    if (this.earlyFireInterval != DEFAULT_FIRE_INTERVAL
      && this.earlyFireInterval != interval.toMilliseconds) {
      // earlyFireInterval of the two query config is not equal and not the default
      throw new RuntimeException(
        "Currently not support different earlyFireInterval configs in one job")
    }
    earlyFireInterval = interval.toMilliseconds
    this
  }

  def getEarlyFireInterval: Long = earlyFireInterval

  /**
    * Specifies the late firing interval in milli second, early fire is the emit strategy
    * after watermark advanced to end of window.
    */
  def withLateFireInterval(interval: Time): TableConfig = {
    if (this.lateFireInterval != DEFAULT_FIRE_INTERVAL
      && this.lateFireInterval != interval.toMilliseconds) {
      // lateFireInterval of the two query config is not equal and not the default
      throw new RuntimeException(
        "Currently not support different lateFireInterval configs in one job")
    }
    lateFireInterval = interval.toMilliseconds
    this
  }

  def getLateFireInterval: Long = lateFireInterval
}

object TableConfig {
  def DEFAULT = new TableConfig()
}

object OperatorType extends Enumeration {
  type OperatorType = Value
  val NestedLoopJoin, ShuffleHashJoin, BroadcastHashJoin, SortMergeJoin, HashAgg, SortAgg = Value
}

object AggPhaseEnforcer extends Enumeration {
  type AggPhaseEnforcer = Value
  val NONE, ONE_PHASE, TWO_PHASE = Value
}
