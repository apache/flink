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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Utilities for API-specific {@link Expression}s.
 */
@Internal
public final class ApiExpressionUtils {

	public static final long MILLIS_PER_SECOND = 1000L;

	public static final long MILLIS_PER_MINUTE = 60000L;

	public static final long MILLIS_PER_HOUR = 3600000L; // = 60 * 60 * 1000

	public static final long MILLIS_PER_DAY = 86400000L; // = 24 * 60 * 60 * 1000

	private ApiExpressionUtils() {
		// private
	}

	/**
	 * Converts a given object to an expression.
	 *
	 * <p>It converts:
	 * <ul>
	 *     <li>{@code null} to null literal</li>
	 *     <li>{@link Row} to a call to a row constructor expression</li>
	 *     <li>{@link Map} to a call to a map constructor expression</li>
	 *     <li>{@link List} to a call to an array constructor expression</li>
	 *     <li>arrays to a call to an array constructor expression</li>
	 *     <li>Scala's {@code Seq} to an array constructor via reflection</li>
	 *     <li>Scala's {@code Map} to a map constructor via reflection</li>
	 *     <li>Scala's {@code BigDecimal} to a DECIMAL literal</li>
	 *     <li>if none of the above applies, the function tries to convert the object
	 *          to a value literal with {@link #valueLiteral(Object)}</li>
	 * </ul>
	 *
	 * @param expression An object to convert to an expression
	 */
	public static Expression objectToExpression(Object expression) {
		if (expression == null) {
			return valueLiteral(null, DataTypes.NULL());
		} else if (expression instanceof ApiExpression) {
			return ((ApiExpression) expression).toExpr();
		} else if (expression instanceof Expression) {
			return (Expression) expression;
		} else if (expression instanceof Row) {
			RowKind kind = ((Row) expression).getKind();
			if (kind != RowKind.INSERT) {
				throw new ValidationException(
					String.format(
						"Unsupported kind '%s' of a row [%s]. Only rows with 'INSERT' kind are supported when" +
							" converting to an expression.", kind, expression));
			}
			return convertRow((Row) expression);
		} else if (expression instanceof Map) {
			return convertJavaMap((Map<?, ?>) expression);
		} else if (expression instanceof byte[]) {
			// BINARY LITERAL
			return valueLiteral(expression);
		} else if (expression.getClass().isArray()) {
			return convertArray(expression);
		} else if (expression instanceof List) {
			return convertJavaList((List<?>) expression);
		} else {
			return convertScala(expression).orElseGet(() -> valueLiteral(expression));
		}
	}

	private static Expression convertRow(Row expression) {
		List<Expression> fields = IntStream.range(0, expression.getArity())
			.mapToObj(expression::getField)
			.map(ApiExpressionUtils::objectToExpression)
			.collect(Collectors.toList());

		return unresolvedCall(BuiltInFunctionDefinitions.ROW, fields);
	}

	private static Expression convertJavaMap(Map<?, ?> expression) {
		List<Expression> entries = expression.entrySet()
			.stream()
			.flatMap(e -> Stream.of(
				objectToExpression(e.getKey()),
				objectToExpression(e.getValue())
			)).collect(Collectors.toList());

		return unresolvedCall(BuiltInFunctionDefinitions.MAP, entries);
	}

	private static Expression convertJavaList(List<?> expression) {
		List<Expression> entries = expression
			.stream()
			.map(ApiExpressionUtils::objectToExpression)
			.collect(Collectors.toList());

		return unresolvedCall(BuiltInFunctionDefinitions.ARRAY, entries);
	}

	private static Expression convertArray(Object expression) {
		int length = Array.getLength(expression);
		List<Expression> entries = IntStream.range(0, length)
			.mapToObj(idx -> Array.get(expression, idx))
			.map(ApiExpressionUtils::objectToExpression)
			.collect(Collectors.toList());
		return unresolvedCall(BuiltInFunctionDefinitions.ARRAY, entries);
	}

	private static Optional<Expression> convertScala(Object obj) {
		try {
			Optional<Expression> array = convertScalaSeq(obj);
			if (array.isPresent()) {
				return array;
			}

			Optional<Expression> bigDecimal = convertScalaBigDecimal(obj);
			if (bigDecimal.isPresent()) {
				return bigDecimal;
			}

			return convertScalaMap(obj);
		} catch (Exception e) {
			return Optional.empty();
		}
	}

	private static Optional<Expression> convertScalaMap(Object obj)
			throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		Class<?> mapClass = Class.forName("scala.collection.Map");
		if (mapClass.isAssignableFrom(obj.getClass())) {
			Class<?> seqClass = Class.forName("scala.collection.Seq");
			Class<?> productClass = Class.forName("scala.Product");
			Method getElement = productClass.getMethod("productElement", int.class);
			Method toSeq = mapClass.getMethod("toSeq");
			Method getMethod = seqClass.getMethod("apply", Object.class);
			Method lengthMethod = seqClass.getMethod("length");

			Object mapAsSeq = toSeq.invoke(obj);
			List<Expression> entries = new ArrayList<>();
			for (int i = 0; i < (Integer) lengthMethod.invoke(mapAsSeq); i++) {
				Object mapEntry = getMethod.invoke(mapAsSeq, i);

				Object key = getElement.invoke(mapEntry, 0);
				Object value = getElement.invoke(mapEntry, 1);
				entries.add(objectToExpression(key));
				entries.add(objectToExpression(value));
			}

			return Optional.of(unresolvedCall(BuiltInFunctionDefinitions.MAP, entries));
		}
		return Optional.empty();
	}

	private static Optional<Expression> convertScalaSeq(Object obj)
			throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		Class<?> seqClass = Class.forName("scala.collection.Seq");
		if (seqClass.isAssignableFrom(obj.getClass())) {
			Method getMethod = seqClass.getMethod("apply", Object.class);
			Method lengthMethod = seqClass.getMethod("length");

			List<Expression> entries = new ArrayList<>();
			for (int i = 0; i < (Integer) lengthMethod.invoke(obj); i++) {
				entries.add(objectToExpression(getMethod.invoke(obj, i)));
			}

			return Optional.of(unresolvedCall(BuiltInFunctionDefinitions.ARRAY, entries));
		}
		return Optional.empty();
	}

	private static Optional<Expression> convertScalaBigDecimal(Object obj)
			throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		Class<?> decimalClass = Class.forName("scala.math.BigDecimal");
		if (decimalClass.equals(obj.getClass())) {
			Method toJava = decimalClass.getMethod("underlying");
			BigDecimal bigDecimal = (BigDecimal) toJava.invoke(obj);
			return Optional.of(valueLiteral(bigDecimal));
		}
		return Optional.empty();
	}

	public static Expression unwrapFromApi(Expression expression) {
		if (expression instanceof ApiExpression) {
			return ((ApiExpression) expression).toExpr();
		} else {
			return expression;
		}
	}

	public static LocalReferenceExpression localRef(String name, DataType dataType) {
		return new LocalReferenceExpression(name, dataType);
	}

	public static ValueLiteralExpression valueLiteral(Object value) {
		return new ValueLiteralExpression(value);
	}

	public static ValueLiteralExpression valueLiteral(Object value, DataType dataType) {
		return new ValueLiteralExpression(value, dataType);
	}

	public static TypeLiteralExpression typeLiteral(DataType dataType) {
		return new TypeLiteralExpression(dataType);
	}

	public static UnresolvedReferenceExpression unresolvedRef(String name) {
		return new UnresolvedReferenceExpression(name);
	}

	public static UnresolvedCallExpression unresolvedCall(
			FunctionIdentifier functionIdentifier,
			FunctionDefinition functionDefinition,
			Expression... args) {
		return unresolvedCall(functionIdentifier, functionDefinition, Arrays.asList(args));
	}

	public static UnresolvedCallExpression unresolvedCall(
			FunctionIdentifier functionIdentifier,
			FunctionDefinition functionDefinition,
			List<Expression> args) {
		return new UnresolvedCallExpression(functionIdentifier, functionDefinition,
			args.stream()
				.map(ApiExpressionUtils::unwrapFromApi)
				.collect(Collectors.toList()));
	}

	public static UnresolvedCallExpression unresolvedCall(FunctionDefinition functionDefinition, Expression... args) {
		return unresolvedCall(functionDefinition, Arrays.asList(args));
	}

	public static UnresolvedCallExpression unresolvedCall(FunctionDefinition functionDefinition, List<Expression> args) {
		return new UnresolvedCallExpression(
			functionDefinition,
			args.stream()
				.map(ApiExpressionUtils::unwrapFromApi)
				.collect(Collectors.toList()));
	}

	public static TableReferenceExpression tableRef(String name, Table table) {
		return tableRef(name, table.getQueryOperation());
	}

	public static TableReferenceExpression tableRef(String name, QueryOperation queryOperation) {
		return new TableReferenceExpression(name, queryOperation);
	}

	public static LookupCallExpression lookupCall(String name, Expression... args) {
		return new LookupCallExpression(
			name,
			Arrays.stream(args)
				.map(ApiExpressionUtils::unwrapFromApi)
				.collect(Collectors.toList()));
	}

	public static Expression toMonthInterval(Expression e, int multiplier) {
		return ExpressionUtils.extractValue(e, BigDecimal.class)
			.map((v) -> intervalOfMonths(v.intValue() * multiplier))
			.orElseThrow(() -> new ValidationException("Invalid constant for year-month interval: " + e));
	}

	public static ValueLiteralExpression intervalOfMillis(long millis) {
		return valueLiteral(
			millis,
			DataTypes.INTERVAL(DataTypes.SECOND(3)).notNull().bridgedTo(Long.class));
	}

	public static Expression toMilliInterval(Expression e, long multiplier) {
		return ExpressionUtils.extractValue(e, BigDecimal.class)
			.map((v) -> intervalOfMillis(v.longValue() * multiplier))
			.orElseThrow(() -> new ValidationException("Invalid constant for day-time interval: " + e));
	}

	public static ValueLiteralExpression intervalOfMonths(int months) {
		return valueLiteral(
			months,
			DataTypes.INTERVAL(DataTypes.MONTH()).notNull().bridgedTo(Integer.class));
	}

	public static Expression toRowInterval(Expression e) {
		return ExpressionUtils.extractValue(e, BigDecimal.class)
			.map(bd -> ApiExpressionUtils.valueLiteral(bd.longValue()))
			.orElseThrow(() -> new ValidationException("Invalid constant for row interval: " + e));
	}

	/**
	 * Checks if the expression is a function call of given type.
	 *
	 * @param expression expression to check
	 * @param kind expected type of function
	 * @return true if the expression is function call of given type, false otherwise
	 */
	public static boolean isFunctionOfKind(Expression expression, FunctionKind kind) {
		if (expression instanceof UnresolvedCallExpression) {
			return ((UnresolvedCallExpression) expression).getFunctionDefinition().getKind() == kind;
		}
		if (expression instanceof CallExpression) {
			return ((CallExpression) expression).getFunctionDefinition().getKind() == kind;
		}
		return false;
	}

	/**
	 * Checks if the given expression is a given builtin function.
	 *
	 * @param expression expression to check
	 * @param functionDefinition expected function definition
	 * @return true if the given expression is a given function call
	 */
	public static boolean isFunction(Expression expression, BuiltInFunctionDefinition functionDefinition) {
		if (expression instanceof UnresolvedCallExpression) {
			return ((UnresolvedCallExpression) expression).getFunctionDefinition() == functionDefinition;
		}
		if (expression instanceof CallExpression) {
			return ((CallExpression) expression).getFunctionDefinition() == functionDefinition;
		}
		return false;
	}
}
