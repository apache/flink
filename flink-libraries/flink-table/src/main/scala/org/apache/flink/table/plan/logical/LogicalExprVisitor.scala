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

package org.apache.flink.table.plan.logical

import org.apache.flink.table.api.{CurrentRange, CurrentRow, UnboundedRange, UnboundedRow}
import org.apache.flink.table.expressions._

trait LogicalExprVisitor[T] {

  // aggregations.scala
  def visit(expression: Expression): T
  def visit(sum: Sum): T
  def visit(sum0: Sum0): T
  def visit(incr_sum: IncrSum): T
  def visit(min: Min): T
  def visit(max: Max): T
  def visit(count: Count): T
  def visit(avg: Avg): T
  def visit(lead: Lead): T
  def visit(lag: Lag): T
  def visit(stddevPop: StddevPop): T
  def visit(stddevSamp: StddevSamp): T
  def visit(stddev: Stddev): T
  def visit(varPop: VarPop): T
  def visit(varSamp: VarSamp): T
  def visit(variance: Variance): T
  def visit(firstValue: FirstValue): T
  def visit(lastValue: LastValue): T
  def visit(aggFunctionCall: AggFunctionCall): T
  def visit(singleValue: SingleValue): T

  // arithmetic.scala
  def visit(plus: Plus): T
  def visit(unaryMinus: UnaryMinus): T
  def visit(minus: org.apache.flink.table.expressions.Minus): T
  def visit(div: Div): T
  def visit(mul: Mul): T
  def visit(mod: Mod): T

  // call.scala
  def visit(call: Call): T
  def visit(unresolvedOverCall: UnresolvedOverCall): T
  def visit(overCall: OverCall): T
  def visit(scalarFunctionCall: ScalarFunctionCall): T
  def visit(tableFunctionCall: TableFunctionCall): T
  def visit(throwException: ThrowException): T

  // cast.scala
  def visit(cast: Cast): T

  // collection.scala
  def visit(rowConstructor: RowConstructor): T
  def visit(arrayConstructor: ArrayConstructor): T
  def visit(mapConstructor: MapConstructor): T
  def visit(arrayElement: ArrayElement): T
  def visit(cardinality: Cardinality): T
  def visit(itemAt: ItemAt): T

  // comparison
  def visit(equalTo: EqualTo): T
  def visit(notEqualTo: NotEqualTo): T
  def visit(greaterThan: GreaterThan): T
  def visit(greaterThanOrEqual: GreaterThanOrEqual): T
  def visit(lessThan: LessThan): T
  def visit(lessThanOrEqual: LessThanOrEqual): T
  def visit(isNull: IsNull): T
  def visit(isNotNull: IsNotNull): T
  def visit(isTrue: IsTrue): T
  def visit(isNotTrue: IsNotTrue): T
  def visit(isFalse: IsFalse): T
  def visit(isNotFalse: IsNotFalse): T
  def visit(between: Between): T
  def visit(notBetween: NotBetween): T

  // composite.scala
  def visit(flattening: Flattening): T
  def visit(getCompositeField: GetCompositeField): T

  // fieldExpression.scala
  def visit(unresolvedFieldReference: UnresolvedFieldReference): T
  def visit(resolvedFieldReference: ResolvedFieldReference): T
  def visit(unresolvedAggBufferReference: UnresolvedAggBufferReference): T
  def visit(resolvedAggInputReference: ResolvedAggInputReference): T
  def visit(resolvedAggBufferReference: ResolvedAggBufferReference): T
  def visit(resolvedAggLocalReference: ResolvedAggLocalReference): T
  def visit(alias: Alias): T
  def visit(unresolvedAlias: UnresolvedAlias): T
  def visit(windowReference: WindowReference): T
  def visit(tableReference: TableReference): T
  def visit(rowtimeAttribute: RowtimeAttribute): T
  def visit(proctimeAttribute: ProctimeAttribute): T

  // literals.scala
  def visit(literal: Literal): T
  def visit(_null: Null): T

  // logic.scala
  def visit(not: Not): T
  def visit(and: And): T
  def visit(or: Or): T
  def visit(_if: If): T

  // mathExpressions.scala
  def visit(abs: Abs): T
  def visit(ceil: Ceil): T
  def visit(exp: Exp): T
  def visit(floor: Floor): T
  def visit(log10: Log10): T
  def visit(ln: Ln): T
  def visit(power: Power): T
  def visit(sqrt: Sqrt): T
  def visit(sin: Sin): T
  def visit(cos: Cos): T
  def visit(tan: Tan): T
  def visit(cot: Cot): T
  def visit(asin: Asin): T
  def visit(acos: Acos): T
  def visit(atan: Atan): T
  def visit(degrees: Degrees): T
  def visit(radians: Radians): T
  def visit(sign: Sign): T
  def visit(round: Round): T
  def visit(pi: Pi): T
  def visit(e: E): T
  def visit(rand: Rand): T
  def visit(randInteger: RandInteger): T

  // ordering.scala
  def visit(asc: Asc): T
  def visit(desc: Desc): T
  def visit(nullsFirst: NullsFirst): T
  def visit(nullsLast: NullsLast): T

  // stringExpressions.scala
  def visit(charLength: CharLength): T
  def visit(initCap: InitCap): T
  def visit(like: Like): T
  def visit(lower: Lower): T
  def visit(similar: Similar): T
  def visit(substring: Substring): T
  def visit(left: Left): T
  def visit(right: Right): T
  def visit(trim: Trim): T
  def visit(ltrim: Ltrim): T
  def visit(rtrim: Rtrim): T
  def visit(upper: Upper): T
  def visit(position: Position): T
  def visit(overlay: Overlay): T
  def visit(concat: Concat): T
  def visit(concatWs: ConcatWs): T
  def visit(locate: Locate): T
  def visit(ascii: Ascii): T
  def visit(encode: Encode): T
  def visit(decode: Decode): T
  def visit(instr: Instr): T

  // hashExpressions.scala
  def visit(hash: HashExpression): T

  // subquery.scala
  def visit(in: In): T

  // symbols.scala
  def visit(symbolExpression: SymbolExpression): T
  def visit(proc: Proctime): T

  // time.scala
  def visit(extract: Extract): T
  def visit(temporalFloor: TemporalFloor): T
  def visit(temporalCeil: TemporalCeil): T
  def visit(currentDate: CurrentDate): T
  def visit(currentTime: CurrentTime): T
  def visit(currentTimestamp: CurrentTimestamp): T
  def visit(localTime: LocalTime): T
  def visit(localTimestamp: LocalTimestamp): T
  def visit(quarter: Quarter): T
  def visit(temporalOverlaps: TemporalOverlaps): T
  def visit(dateFormat: DateFormat): T

  // windowProperties.scala
  def visit(windowStart: WindowStart): T
  def visit(windowEnd: WindowEnd): T

  // logicalWindow
  def visit(logicalWindow: LogicalWindow): T
  def visit(tumblingGroupWindow: TumblingGroupWindow): T
  def visit(slidingGroupWindow: SlidingGroupWindow): T
  def visit(sessionGroupWindow: SessionGroupWindow): T

  def visit(currentRow: CurrentRow): T
  def visit(currentRange: CurrentRange): T
  def visit(unboundedRow: UnboundedRow): T
  def visit(unboundedRange: UnboundedRange): T

}
