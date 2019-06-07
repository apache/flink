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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.TableException;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.table.functions.FunctionDefinition.Type.AGGREGATE_FUNCTION;
import static org.apache.flink.table.functions.FunctionDefinition.Type.OTHER_FUNCTION;
import static org.apache.flink.table.functions.FunctionDefinition.Type.SCALAR_FUNCTION;

/**
 * Dictionary of function definitions for all built-in functions.
 */
@PublicEvolving
public final class BuiltInFunctionDefinitions {

	// logic functions
	public static final FunctionDefinition AND =
		new FunctionDefinition("and", SCALAR_FUNCTION);
	public static final FunctionDefinition OR =
		new FunctionDefinition("or", SCALAR_FUNCTION);
	public static final FunctionDefinition NOT =
		new FunctionDefinition("not", SCALAR_FUNCTION);
	public static final FunctionDefinition IF =
		new FunctionDefinition("ifThenElse", SCALAR_FUNCTION);

	// comparison functions
	public static final FunctionDefinition EQUALS =
		new FunctionDefinition("equals", SCALAR_FUNCTION);
	public static final FunctionDefinition GREATER_THAN =
		new FunctionDefinition("greaterThan", SCALAR_FUNCTION);
	public static final FunctionDefinition GREATER_THAN_OR_EQUAL =
		new FunctionDefinition("greaterThanOrEqual", SCALAR_FUNCTION);
	public static final FunctionDefinition LESS_THAN =
		new FunctionDefinition("lessThan", SCALAR_FUNCTION);
	public static final FunctionDefinition LESS_THAN_OR_EQUAL =
		new FunctionDefinition("lessThanOrEqual", SCALAR_FUNCTION);
	public static final FunctionDefinition NOT_EQUALS =
		new FunctionDefinition("notEquals", SCALAR_FUNCTION);
	public static final FunctionDefinition IS_NULL =
		new FunctionDefinition("isNull", SCALAR_FUNCTION);
	public static final FunctionDefinition IS_NOT_NULL =
		new FunctionDefinition("isNotNull", SCALAR_FUNCTION);
	public static final FunctionDefinition IS_TRUE =
		new FunctionDefinition("isTrue", SCALAR_FUNCTION);
	public static final FunctionDefinition IS_FALSE =
		new FunctionDefinition("isFalse", SCALAR_FUNCTION);
	public static final FunctionDefinition IS_NOT_TRUE =
		new FunctionDefinition("isNotTrue", SCALAR_FUNCTION);
	public static final FunctionDefinition IS_NOT_FALSE =
		new FunctionDefinition("isNotFalse", SCALAR_FUNCTION);
	public static final FunctionDefinition BETWEEN =
		new FunctionDefinition("between", SCALAR_FUNCTION);
	public static final FunctionDefinition NOT_BETWEEN =
		new FunctionDefinition("notBetween", SCALAR_FUNCTION);

	// aggregate functions
	public static final FunctionDefinition AVG =
		new FunctionDefinition("avg", AGGREGATE_FUNCTION);
	public static final FunctionDefinition COUNT =
		new FunctionDefinition("count", AGGREGATE_FUNCTION);
	public static final FunctionDefinition MAX =
		new FunctionDefinition("max", AGGREGATE_FUNCTION);
	public static final FunctionDefinition MIN =
		new FunctionDefinition("min", AGGREGATE_FUNCTION);
	public static final FunctionDefinition SUM =
		new FunctionDefinition("sum", AGGREGATE_FUNCTION);
	public static final FunctionDefinition SUM0 =
		new FunctionDefinition("sum0", AGGREGATE_FUNCTION);
	public static final FunctionDefinition STDDEV_POP =
		new FunctionDefinition("stddevPop", AGGREGATE_FUNCTION);
	public static final FunctionDefinition STDDEV_SAMP =
		new FunctionDefinition("stddevSamp", AGGREGATE_FUNCTION);
	public static final FunctionDefinition VAR_POP =
		new FunctionDefinition("varPop", AGGREGATE_FUNCTION);
	public static final FunctionDefinition VAR_SAMP =
		new FunctionDefinition("varSamp", AGGREGATE_FUNCTION);
	public static final FunctionDefinition COLLECT =
		new FunctionDefinition("collect", AGGREGATE_FUNCTION);
	public static final FunctionDefinition DISTINCT =
		new FunctionDefinition("distinct", AGGREGATE_FUNCTION);

	// string functions
	public static final FunctionDefinition CHAR_LENGTH =
		new FunctionDefinition("charLength", SCALAR_FUNCTION);
	public static final FunctionDefinition INIT_CAP =
		new FunctionDefinition("initCap", SCALAR_FUNCTION);
	public static final FunctionDefinition LIKE =
		new FunctionDefinition("like", SCALAR_FUNCTION);
	public static final FunctionDefinition LOWER =
		new FunctionDefinition("lowerCase", SCALAR_FUNCTION);
	public static final FunctionDefinition SIMILAR =
		new FunctionDefinition("similar", SCALAR_FUNCTION);
	public static final FunctionDefinition SUBSTRING =
		new FunctionDefinition("substring", SCALAR_FUNCTION);
	public static final FunctionDefinition REPLACE =
		new FunctionDefinition("replace", SCALAR_FUNCTION);
	public static final FunctionDefinition TRIM =
		new FunctionDefinition("trim", SCALAR_FUNCTION);
	public static final FunctionDefinition UPPER =
		new FunctionDefinition("upperCase", SCALAR_FUNCTION);
	public static final FunctionDefinition POSITION =
		new FunctionDefinition("position", SCALAR_FUNCTION);
	public static final FunctionDefinition OVERLAY =
		new FunctionDefinition("overlay", SCALAR_FUNCTION);
	public static final FunctionDefinition CONCAT =
		new FunctionDefinition("concat", SCALAR_FUNCTION);
	public static final FunctionDefinition CONCAT_WS =
		new FunctionDefinition("concat_ws", SCALAR_FUNCTION);
	public static final FunctionDefinition LPAD =
		new FunctionDefinition("lpad", SCALAR_FUNCTION);
	public static final FunctionDefinition RPAD =
		new FunctionDefinition("rpad", SCALAR_FUNCTION);
	public static final FunctionDefinition REGEXP_EXTRACT =
		new FunctionDefinition("regexpExtract", SCALAR_FUNCTION);
	public static final FunctionDefinition FROM_BASE64 =
		new FunctionDefinition("fromBase64", SCALAR_FUNCTION);
	public static final FunctionDefinition TO_BASE64 =
		new FunctionDefinition("toBase64", SCALAR_FUNCTION);
	public static final FunctionDefinition UUID =
		new FunctionDefinition("uuid", SCALAR_FUNCTION);
	public static final FunctionDefinition LTRIM =
		new FunctionDefinition("ltrim", SCALAR_FUNCTION);
	public static final FunctionDefinition RTRIM =
		new FunctionDefinition("rtrim", SCALAR_FUNCTION);
	public static final FunctionDefinition REPEAT =
		new FunctionDefinition("repeat", SCALAR_FUNCTION);
	public static final FunctionDefinition REGEXP_REPLACE =
		new FunctionDefinition("regexpReplace", SCALAR_FUNCTION);

	// math functions
	public static final FunctionDefinition PLUS =
		new FunctionDefinition("plus", SCALAR_FUNCTION);
	public static final FunctionDefinition MINUS =
		new FunctionDefinition("minus", SCALAR_FUNCTION);
	public static final FunctionDefinition DIVIDE =
		new FunctionDefinition("divide", SCALAR_FUNCTION);
	public static final FunctionDefinition TIMES =
		new FunctionDefinition("times", SCALAR_FUNCTION);
	public static final FunctionDefinition ABS =
		new FunctionDefinition("abs", SCALAR_FUNCTION);
	public static final FunctionDefinition CEIL =
		new FunctionDefinition("ceil", SCALAR_FUNCTION);
	public static final FunctionDefinition EXP =
		new FunctionDefinition("exp", SCALAR_FUNCTION);
	public static final FunctionDefinition FLOOR =
		new FunctionDefinition("floor", SCALAR_FUNCTION);
	public static final FunctionDefinition LOG10 =
		new FunctionDefinition("log10", SCALAR_FUNCTION);
	public static final FunctionDefinition LOG2 =
		new FunctionDefinition("log2", SCALAR_FUNCTION);
	public static final FunctionDefinition LN =
		new FunctionDefinition("ln", SCALAR_FUNCTION);
	public static final FunctionDefinition LOG =
		new FunctionDefinition("log", SCALAR_FUNCTION);
	public static final FunctionDefinition POWER =
		new FunctionDefinition("power", SCALAR_FUNCTION);
	public static final FunctionDefinition MOD =
		new FunctionDefinition("mod", SCALAR_FUNCTION);
	public static final FunctionDefinition SQRT =
		new FunctionDefinition("sqrt", SCALAR_FUNCTION);
	public static final FunctionDefinition MINUS_PREFIX =
		new FunctionDefinition("minusPrefix", SCALAR_FUNCTION);
	public static final FunctionDefinition SIN =
		new FunctionDefinition("sin", SCALAR_FUNCTION);
	public static final FunctionDefinition COS =
		new FunctionDefinition("cos", SCALAR_FUNCTION);
	public static final FunctionDefinition SINH =
		new FunctionDefinition("sinh", SCALAR_FUNCTION);
	public static final FunctionDefinition TAN =
		new FunctionDefinition("tan", SCALAR_FUNCTION);
	public static final FunctionDefinition TANH =
		new FunctionDefinition("tanh", SCALAR_FUNCTION);
	public static final FunctionDefinition COT =
		new FunctionDefinition("cot", SCALAR_FUNCTION);
	public static final FunctionDefinition ASIN =
		new FunctionDefinition("asin", SCALAR_FUNCTION);
	public static final FunctionDefinition ACOS =
		new FunctionDefinition("acos", SCALAR_FUNCTION);
	public static final FunctionDefinition ATAN =
		new FunctionDefinition("atan", SCALAR_FUNCTION);
	public static final FunctionDefinition ATAN2 =
		new FunctionDefinition("atan2", SCALAR_FUNCTION);
	public static final FunctionDefinition COSH =
		new FunctionDefinition("cosh", SCALAR_FUNCTION);
	public static final FunctionDefinition DEGREES =
		new FunctionDefinition("degrees", SCALAR_FUNCTION);
	public static final FunctionDefinition RADIANS =
		new FunctionDefinition("radians", SCALAR_FUNCTION);
	public static final FunctionDefinition SIGN =
		new FunctionDefinition("sign", SCALAR_FUNCTION);
	public static final FunctionDefinition ROUND =
		new FunctionDefinition("round", SCALAR_FUNCTION);
	public static final FunctionDefinition PI =
		new FunctionDefinition("pi", SCALAR_FUNCTION);
	public static final FunctionDefinition E =
		new FunctionDefinition("e", SCALAR_FUNCTION);
	public static final FunctionDefinition RAND =
		new FunctionDefinition("rand", SCALAR_FUNCTION);
	public static final FunctionDefinition RAND_INTEGER =
		new FunctionDefinition("randInteger", SCALAR_FUNCTION);
	public static final FunctionDefinition BIN =
		new FunctionDefinition("bin", SCALAR_FUNCTION);
	public static final FunctionDefinition HEX =
		new FunctionDefinition("hex", SCALAR_FUNCTION);
	public static final FunctionDefinition TRUNCATE =
		new FunctionDefinition("truncate", SCALAR_FUNCTION);

	// time functions
	public static final FunctionDefinition EXTRACT =
		new FunctionDefinition("extract", SCALAR_FUNCTION);
	public static final FunctionDefinition CURRENT_DATE =
		new FunctionDefinition("currentDate", SCALAR_FUNCTION);
	public static final FunctionDefinition CURRENT_TIME =
		new FunctionDefinition("currentTime", SCALAR_FUNCTION);
	public static final FunctionDefinition CURRENT_TIMESTAMP =
		new FunctionDefinition("currentTimestamp", SCALAR_FUNCTION);
	public static final FunctionDefinition LOCAL_TIME =
		new FunctionDefinition("localTime", SCALAR_FUNCTION);
	public static final FunctionDefinition LOCAL_TIMESTAMP =
		new FunctionDefinition("localTimestamp", SCALAR_FUNCTION);
	public static final FunctionDefinition TEMPORAL_OVERLAPS =
		new FunctionDefinition("temporalOverlaps", SCALAR_FUNCTION);
	public static final FunctionDefinition DATE_TIME_PLUS =
		new FunctionDefinition("dateTimePlus", SCALAR_FUNCTION);
	public static final FunctionDefinition DATE_FORMAT =
		new FunctionDefinition("dateFormat", SCALAR_FUNCTION);
	public static final FunctionDefinition TIMESTAMP_DIFF =
		new FunctionDefinition("timestampDiff", SCALAR_FUNCTION);

	// collection
	public static final FunctionDefinition AT =
		new FunctionDefinition("at", SCALAR_FUNCTION);
	public static final FunctionDefinition CARDINALITY =
		new FunctionDefinition("cardinality", SCALAR_FUNCTION);
	public static final FunctionDefinition ARRAY =
		new FunctionDefinition("array", SCALAR_FUNCTION);
	public static final FunctionDefinition ARRAY_ELEMENT =
		new FunctionDefinition("element", SCALAR_FUNCTION);
	public static final FunctionDefinition MAP =
		new FunctionDefinition("map", SCALAR_FUNCTION);
	public static final FunctionDefinition ROW =
		new FunctionDefinition("row", SCALAR_FUNCTION);

	// composite
	public static final FunctionDefinition FLATTEN =
		new FunctionDefinition("flatten", OTHER_FUNCTION);
	public static final FunctionDefinition GET =
		new FunctionDefinition("get", OTHER_FUNCTION);

	// window properties
	public static final FunctionDefinition WINDOW_START =
		new FunctionDefinition("start", OTHER_FUNCTION);
	public static final FunctionDefinition WINDOW_END =
		new FunctionDefinition("end", OTHER_FUNCTION);

	// ordering
	public static final FunctionDefinition ORDER_ASC =
		new FunctionDefinition("asc", OTHER_FUNCTION);
	public static final FunctionDefinition ORDER_DESC =
		new FunctionDefinition("desc", OTHER_FUNCTION);

	// crypto hash
	public static final FunctionDefinition MD5 =
		new FunctionDefinition("md5", SCALAR_FUNCTION);
	public static final FunctionDefinition SHA1 =
		new FunctionDefinition("sha1", SCALAR_FUNCTION);
	public static final FunctionDefinition SHA224 =
		new FunctionDefinition("sha224", SCALAR_FUNCTION);
	public static final FunctionDefinition SHA256 =
		new FunctionDefinition("sha256", SCALAR_FUNCTION);
	public static final FunctionDefinition SHA384 =
		new FunctionDefinition("sha384", SCALAR_FUNCTION);
	public static final FunctionDefinition SHA512 =
		new FunctionDefinition("sha512", SCALAR_FUNCTION);
	public static final FunctionDefinition SHA2 =
		new FunctionDefinition("sha2", SCALAR_FUNCTION);

	// time attributes
	public static final FunctionDefinition PROCTIME =
		new FunctionDefinition("proctime", OTHER_FUNCTION);
	public static final FunctionDefinition ROWTIME =
		new FunctionDefinition("rowtime", OTHER_FUNCTION);

	// over window
	public static final FunctionDefinition OVER =
		new FunctionDefinition("over", OTHER_FUNCTION);
	public static final FunctionDefinition UNBOUNDED_RANGE =
		new FunctionDefinition("unboundedRange", OTHER_FUNCTION);
	public static final FunctionDefinition UNBOUNDED_ROW =
		new FunctionDefinition("unboundedRow", OTHER_FUNCTION);
	public static final FunctionDefinition CURRENT_RANGE =
		new FunctionDefinition("currentRange", OTHER_FUNCTION);
	public static final FunctionDefinition CURRENT_ROW =
		new FunctionDefinition("currentRow", OTHER_FUNCTION);

	// columns
	public static final FunctionDefinition WITH_COLUMNS =
		new FunctionDefinition("withColumns", OTHER_FUNCTION);
	public static final FunctionDefinition WITHOUT_COLUMNS =
		new FunctionDefinition("withoutColumns", OTHER_FUNCTION);

	// etc
	public static final FunctionDefinition IN =
		new FunctionDefinition("in", SCALAR_FUNCTION);
	public static final FunctionDefinition CAST =
		new FunctionDefinition("cast", SCALAR_FUNCTION);
	public static final FunctionDefinition REINTERPRET_CAST =
			new FunctionDefinition("reinterpretCast", SCALAR_FUNCTION);
	public static final FunctionDefinition AS =
		new FunctionDefinition("as", OTHER_FUNCTION);
	public static final FunctionDefinition STREAM_RECORD_TIMESTAMP =
		new FunctionDefinition("streamRecordTimestamp", OTHER_FUNCTION);
	public static final FunctionDefinition RANGE_TO =
		new FunctionDefinition("rangeTo", OTHER_FUNCTION);

	public static final Set<FunctionDefinition> WINDOW_PROPERTIES = new HashSet<>(Arrays.asList(
		WINDOW_START, WINDOW_END, PROCTIME, ROWTIME
	));

	public static final Set<FunctionDefinition> TIME_ATTRIBUTES = new HashSet<>(Arrays.asList(
		PROCTIME, ROWTIME
	));

	public static final List<FunctionDefinition> ORDERING = Arrays.asList(ORDER_ASC, ORDER_DESC);

	public static List<FunctionDefinition> getDefinitions() {
		final Field[] fields = BuiltInFunctionDefinitions.class.getFields();
		final List<FunctionDefinition> list = new ArrayList<>(fields.length);
		for (Field field : fields) {
			if (FunctionDefinition.class.isAssignableFrom(field.getType())) {
				try {
					final FunctionDefinition funcDef = (FunctionDefinition) field.get(BuiltInFunctionDefinitions.class);
					list.add(Preconditions.checkNotNull(funcDef));
				} catch (IllegalAccessException e) {
					throw new TableException(
						"The function definition for field " + field.getName() + " is not accessible.", e);
				}
			}
		}
		return list;
	}
}
