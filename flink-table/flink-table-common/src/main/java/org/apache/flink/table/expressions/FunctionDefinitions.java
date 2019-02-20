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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.TableException;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

import static org.apache.flink.table.expressions.FunctionType.AGGREGATION;
import static org.apache.flink.table.expressions.FunctionType.BETWEEN_COMPARISON;
import static org.apache.flink.table.expressions.FunctionType.BINARY_COMPARISON;
import static org.apache.flink.table.expressions.FunctionType.COLLECTION;
import static org.apache.flink.table.expressions.FunctionType.COMPOSITE;
import static org.apache.flink.table.expressions.FunctionType.HASH;
import static org.apache.flink.table.expressions.FunctionType.LOGICAL;
import static org.apache.flink.table.expressions.FunctionType.ORDERING;
import static org.apache.flink.table.expressions.FunctionType.OTHER;
import static org.apache.flink.table.expressions.FunctionType.OVER_WINDOW;
import static org.apache.flink.table.expressions.FunctionType.SUBQUERY;
import static org.apache.flink.table.expressions.FunctionType.TIME;
import static org.apache.flink.table.expressions.FunctionType.TIME_ATTRIBUTE;
import static org.apache.flink.table.expressions.FunctionType.UNARY_COMPARISON;
import static org.apache.flink.table.expressions.FunctionType.WINDOW_PROPERTY;

/**
 * FunctionDefinitions is like a dictionary, defining all FunctionDefinitions as constants.
 */
@PublicEvolving
public final class FunctionDefinitions {

	public static final FunctionDefinition CAST = new FunctionDefinition("cast", OTHER);
	public static final FunctionDefinition AS = new FunctionDefinition("as", OTHER);
	public static final FunctionDefinition STREAM_RECORD_TIMESTAMP =
		new FunctionDefinition("streamRecordTimestamp", OTHER);

	// composite
	public static final FunctionDefinition FLATTEN = new FunctionDefinition("flatten", COMPOSITE);
	public static final FunctionDefinition GET_COMPOSITE_FIELD = new FunctionDefinition("getCompositeField", COMPOSITE);

	// logic functions
	public static final FunctionDefinition AND = new FunctionDefinition("and", LOGICAL);
	public static final FunctionDefinition OR = new FunctionDefinition("or", LOGICAL);
	public static final FunctionDefinition NOT = new FunctionDefinition("not", LOGICAL);

	public static final FunctionDefinition IF = new FunctionDefinition("if", OTHER);

	// comparison functions
	public static final FunctionDefinition EQUALS = new FunctionDefinition("equals", BINARY_COMPARISON);
	public static final FunctionDefinition GREATER_THAN = new FunctionDefinition("greaterThan", BINARY_COMPARISON);
	public static final FunctionDefinition GREATER_THAN_OR_EQUAL =
		new FunctionDefinition("greaterThanOrEqual", BINARY_COMPARISON);
	public static final FunctionDefinition LESS_THAN = new FunctionDefinition("lessThan", BINARY_COMPARISON);
	public static final FunctionDefinition LESS_THAN_OR_EQUAL =
		new FunctionDefinition("lessThanOrEqual", BINARY_COMPARISON);
	public static final FunctionDefinition NOT_EQUALS = new FunctionDefinition("notEquals", BINARY_COMPARISON);

	public static final FunctionDefinition IS_NULL = new FunctionDefinition("isNull", UNARY_COMPARISON);
	public static final FunctionDefinition IS_NOT_NULL = new FunctionDefinition("isNotNull", UNARY_COMPARISON);
	public static final FunctionDefinition IS_TRUE = new FunctionDefinition("isTrue", UNARY_COMPARISON);
	public static final FunctionDefinition IS_FALSE = new FunctionDefinition("isFalse", UNARY_COMPARISON);
	public static final FunctionDefinition IS_NOT_TRUE = new FunctionDefinition("isNotTrue", UNARY_COMPARISON);
	public static final FunctionDefinition IS_NOT_FALSE = new FunctionDefinition("isNotFalse", UNARY_COMPARISON);
	public static final FunctionDefinition BETWEEN = new FunctionDefinition("between", BETWEEN_COMPARISON);
	public static final FunctionDefinition NOT_BETWEEN = new FunctionDefinition("notBetween", BETWEEN_COMPARISON);

	// subquery
	public static final FunctionDefinition IN = new FunctionDefinition("in", SUBQUERY);

	// aggregate functions
	public static final FunctionDefinition AVG = new FunctionDefinition("avg", AGGREGATION);
	public static final FunctionDefinition COUNT = new FunctionDefinition("count", AGGREGATION);
	public static final FunctionDefinition MAX = new FunctionDefinition("max", AGGREGATION);
	public static final FunctionDefinition MIN = new FunctionDefinition("min", AGGREGATION);
	public static final FunctionDefinition SUM = new FunctionDefinition("sum", AGGREGATION);
	public static final FunctionDefinition SUM0 = new FunctionDefinition("sum0", AGGREGATION);
	public static final FunctionDefinition STDDEV_POP = new FunctionDefinition("stddevPop", AGGREGATION);
	public static final FunctionDefinition STDDEV_SAMP = new FunctionDefinition("stddevSamp", AGGREGATION);
	public static final FunctionDefinition VAR_POP = new FunctionDefinition("varPop", AGGREGATION);
	public static final FunctionDefinition VAR_SAMP = new FunctionDefinition("varSamp", AGGREGATION);
	public static final FunctionDefinition COLLECT = new FunctionDefinition("collect", AGGREGATION);
	public static final FunctionDefinition DISTINCT = new FunctionDefinition("distinct", AGGREGATION);

	// string functions
	public static final FunctionDefinition CHAR_LENGTH = new FunctionDefinition("charLength", OTHER);
	public static final FunctionDefinition INIT_CAP = new FunctionDefinition("initCap", OTHER);
	public static final FunctionDefinition LIKE = new FunctionDefinition("like", OTHER);
	public static final FunctionDefinition LOWER = new FunctionDefinition("lower", OTHER);
	public static final FunctionDefinition SIMILAR = new FunctionDefinition("similar", OTHER);
	public static final FunctionDefinition SUBSTRING = new FunctionDefinition("substring", OTHER);
	public static final FunctionDefinition REPLACE = new FunctionDefinition("replace", OTHER);
	public static final FunctionDefinition TRIM = new FunctionDefinition("trim", OTHER);
	public static final FunctionDefinition UPPER = new FunctionDefinition("upper", OTHER);
	public static final FunctionDefinition POSITION = new FunctionDefinition("position", OTHER);
	public static final FunctionDefinition OVERLAY = new FunctionDefinition("overlay", OTHER);
	public static final FunctionDefinition CONCAT = new FunctionDefinition("concat", OTHER);
	public static final FunctionDefinition CONCAT_WS = new FunctionDefinition("concat_ws", OTHER);
	public static final FunctionDefinition LPAD = new FunctionDefinition("lpad", OTHER);
	public static final FunctionDefinition RPAD = new FunctionDefinition("rpad", OTHER);
	public static final FunctionDefinition REGEXP_EXTRACT = new FunctionDefinition("regexpExtract", OTHER);
	public static final FunctionDefinition FROM_BASE64 = new FunctionDefinition("fromBase64", OTHER);
	public static final FunctionDefinition TO_BASE64 = new FunctionDefinition("toBase64", OTHER);
	public static final FunctionDefinition UUID = new FunctionDefinition("uuid", OTHER);
	public static final FunctionDefinition LTRIM = new FunctionDefinition("ltrim", OTHER);
	public static final FunctionDefinition RTRIM = new FunctionDefinition("rtrim", OTHER);
	public static final FunctionDefinition REPEAT = new FunctionDefinition("repeat", OTHER);
	public static final FunctionDefinition REGEXP_REPLACE = new FunctionDefinition("regexpReplace", OTHER);

	// math functions
	public static final FunctionDefinition PLUS = new FunctionDefinition("plus", OTHER);
	public static final FunctionDefinition MINUS = new FunctionDefinition("minus", OTHER);
	public static final FunctionDefinition DIVIDE = new FunctionDefinition("divide", OTHER);
	public static final FunctionDefinition TIMES = new FunctionDefinition("times", OTHER);
	public static final FunctionDefinition ABS = new FunctionDefinition("abs", OTHER);
	public static final FunctionDefinition CEIL = new FunctionDefinition("ceil", OTHER);
	public static final FunctionDefinition EXP = new FunctionDefinition("exp", OTHER);
	public static final FunctionDefinition FLOOR = new FunctionDefinition("floor", OTHER);
	public static final FunctionDefinition LOG10 = new FunctionDefinition("log10", OTHER);
	public static final FunctionDefinition LOG2 = new FunctionDefinition("log2", OTHER);
	public static final FunctionDefinition LN = new FunctionDefinition("ln", OTHER);
	public static final FunctionDefinition LOG = new FunctionDefinition("log", OTHER);
	public static final FunctionDefinition POWER = new FunctionDefinition("power", OTHER);
	public static final FunctionDefinition MOD = new FunctionDefinition("mod", OTHER);
	public static final FunctionDefinition SQRT = new FunctionDefinition("sqrt", OTHER);
	public static final FunctionDefinition MINUS_PREFIX = new FunctionDefinition("minusPrefix", OTHER);
	public static final FunctionDefinition SIN = new FunctionDefinition("sin", OTHER);
	public static final FunctionDefinition COS = new FunctionDefinition("cos", OTHER);
	public static final FunctionDefinition SINH = new FunctionDefinition("sinh", OTHER);
	public static final FunctionDefinition TAN = new FunctionDefinition("tan", OTHER);
	public static final FunctionDefinition TANH = new FunctionDefinition("tanh", OTHER);
	public static final FunctionDefinition COT = new FunctionDefinition("cot", OTHER);
	public static final FunctionDefinition ASIN = new FunctionDefinition("asin", OTHER);
	public static final FunctionDefinition ACOS = new FunctionDefinition("acos", OTHER);
	public static final FunctionDefinition ATAN = new FunctionDefinition("atan", OTHER);
	public static final FunctionDefinition ATAN2 = new FunctionDefinition("atan2", OTHER);
	public static final FunctionDefinition COSH = new FunctionDefinition("cosh", OTHER);
	public static final FunctionDefinition DEGREES = new FunctionDefinition("degrees", OTHER);
	public static final FunctionDefinition RADIANS = new FunctionDefinition("radians", OTHER);
	public static final FunctionDefinition SIGN = new FunctionDefinition("sign", OTHER);
	public static final FunctionDefinition ROUND = new FunctionDefinition("round", OTHER);
	public static final FunctionDefinition PI = new FunctionDefinition("pi", OTHER);
	public static final FunctionDefinition E = new FunctionDefinition("e", OTHER);
	public static final FunctionDefinition RAND = new FunctionDefinition("rand", OTHER);
	public static final FunctionDefinition RAND_INTEGER = new FunctionDefinition("randInteger", OTHER);
	public static final FunctionDefinition BIN = new FunctionDefinition("bin", OTHER);
	public static final FunctionDefinition HEX = new FunctionDefinition("hex", OTHER);
	public static final FunctionDefinition TRUNCATE = new FunctionDefinition("truncate", OTHER);

	// time functions
	public static final FunctionDefinition EXTRACT = new FunctionDefinition("extract", TIME);
	public static final FunctionDefinition CURRENT_DATE = new FunctionDefinition("currentDate", TIME);
	public static final FunctionDefinition CURRENT_TIME = new FunctionDefinition("currentTime", TIME);
	public static final FunctionDefinition CURRENT_TIMESTAMP = new FunctionDefinition("currentTimestamp", TIME);
	public static final FunctionDefinition LOCAL_TIME = new FunctionDefinition("localTime", TIME);
	public static final FunctionDefinition LOCAL_TIMESTAMP = new FunctionDefinition("localTimestamp", TIME);
	public static final FunctionDefinition QUARTER = new FunctionDefinition("quarter", TIME);
	public static final FunctionDefinition TEMPORAL_OVERLAPS = new FunctionDefinition("temporalOverlaps", TIME);
	public static final FunctionDefinition DATE_TIME_PLUS = new FunctionDefinition("dateTimePlus", TIME);
	public static final FunctionDefinition DATE_FORMAT = new FunctionDefinition("dateFormat", TIME);
	public static final FunctionDefinition TIMESTAMP_DIFF = new FunctionDefinition("timestampDiff", TIME);
	public static final FunctionDefinition TEMPORAL_FLOOR = new FunctionDefinition("temporalFloor", TIME);
	public static final FunctionDefinition TEMPORAL_CEIL = new FunctionDefinition("temporalCeil", TIME);

	// collection

	// item
	public static final FunctionDefinition AT = new FunctionDefinition("at", COLLECTION);
	// cardinality
	public static final FunctionDefinition CARDINALITY = new FunctionDefinition("cardinality", COLLECTION);
	// array
	public static final FunctionDefinition ARRAY = new FunctionDefinition("array", COLLECTION);
	public static final FunctionDefinition ARRAY_ELEMENT = new FunctionDefinition("element", COLLECTION);
	// map
	public static final FunctionDefinition MAP = new FunctionDefinition("map", COLLECTION);
	// row
	public static final FunctionDefinition ROW = new FunctionDefinition("row", COLLECTION);

	// window properties
	public static final FunctionDefinition WIN_START = new FunctionDefinition("start", WINDOW_PROPERTY);
	public static final FunctionDefinition WIN_END = new FunctionDefinition("end", WINDOW_PROPERTY);

	// ordering
	public static final FunctionDefinition ASC = new FunctionDefinition("asc", ORDERING);
	public static final FunctionDefinition DESC = new FunctionDefinition("desc", ORDERING);

	// crypto hash
	public static final FunctionDefinition MD5 = new FunctionDefinition("md5", HASH);
	public static final FunctionDefinition SHA1 = new FunctionDefinition("sha1", HASH);
	public static final FunctionDefinition SHA224 = new FunctionDefinition("sha224", HASH);
	public static final FunctionDefinition SHA256 = new FunctionDefinition("sha256", HASH);
	public static final FunctionDefinition SHA384 = new FunctionDefinition("sha384", HASH);
	public static final FunctionDefinition SHA512 = new FunctionDefinition("sha512", HASH);
	public static final FunctionDefinition SHA2 = new FunctionDefinition("sha2", HASH);

	// time attribute
	public static final FunctionDefinition PROC_TIME = new FunctionDefinition("proctime", TIME_ATTRIBUTE);
	public static final FunctionDefinition ROW_TIME = new FunctionDefinition("rowtime", TIME_ATTRIBUTE);

	// over window
	public static final FunctionDefinition OVER_CALL = new FunctionDefinition("overCall", OVER_WINDOW);
	public static final FunctionDefinition UNBOUNDED_RANGE = new FunctionDefinition("unboundedRange", OVER_WINDOW);
	public static final FunctionDefinition UNBOUNDED_ROW = new FunctionDefinition("unboundedRow", OVER_WINDOW);
	public static final FunctionDefinition CURRENT_RANGE = new FunctionDefinition("currentRange", OVER_WINDOW);
	public static final FunctionDefinition CURRENT_ROW = new FunctionDefinition("currentRow", OVER_WINDOW);

	public static List<FunctionDefinition> getDefinitions() {
		List<FunctionDefinition> list = new LinkedList<>();
		for (Field field : FunctionDefinitions.class.getFields()) {
			if (FunctionDefinition.class.isAssignableFrom(field.getType())) {
				try {
					FunctionDefinition funcDef = (FunctionDefinition) field.get(FunctionDefinitions.class);
						list.add(funcDef);
				} catch (IllegalAccessException e) {
					throw new TableException(
						"The FunctionDefinition defined in FunctionDefinitions must be accessible.");
				}
			}
		}
		return list;
	}

	public static FunctionDefinition getDefinition(String name) {
		List<FunctionDefinition> definitions = getDefinitions();
		for (FunctionDefinition definition : definitions) {
			if (definition.getName().equalsIgnoreCase(name)) {
				return definition;
			}
		}

		return null;
	}
}
