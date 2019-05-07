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

package org.apache.flink.table.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.ApiExpressionDefaultVisitor;
import org.apache.flink.table.expressions.BuiltInFunctionDefinitions;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionUtils;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.types.Row;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.TIME_ATTRIBUTES;

/**
 * Utility classes for extracting names and indices of fields from different {@link TypeInformation}s.
 */
public class FieldInfoUtils {

	/**
	 * Describes extracted fields and corresponding indices from a {@link TypeInformation}.
	 */
	public static class FieldsInfo {
		private final String[] fieldNames;
		private final int[] indices;

		FieldsInfo(String[] fieldNames, int[] indices) {
			this.fieldNames = fieldNames;
			this.indices = indices;
		}

		public String[] getFieldNames() {
			return fieldNames;
		}

		public int[] getIndices() {
			return indices;
		}
	}

	/**
	 * Reference input fields by name:
	 * All fields in the schema definition are referenced by name
	 * (and possibly renamed using an alias (as). In this mode, fields can be reordered and
	 * projected out. Moreover, we can define proctime and rowtime attributes at arbitrary
	 * positions using arbitrary names (except those that exist in the result schema). This mode
	 * can be used for any input type, including POJOs.
	 *
	 * <p>Reference input fields by position:
	 * In this mode, fields are simply renamed. Event-time attributes can
	 * replace the field on their position in the input data (if it is of correct type) or be
	 * appended at the end. Proctime attributes must be appended at the end. This mode can only be
	 * used if the input type has a defined field order (tuple, case class, Row) and no of fields
	 * references a field of the input type.
	 */
	public static boolean isReferenceByPosition(CompositeType<?> ct, Expression[] fields) {
		if (!(ct instanceof TupleTypeInfoBase)) {
			return false;
		}

		List<String> inputNames = Arrays.asList(ct.getFieldNames());

		// Use the by-position mode if no of the fields exists in the input.
		// This prevents confusing cases like ('f2, 'f0, 'myName) for a Tuple3 where fields are renamed
		// by position but the user might assume reordering instead of renaming.
		return Arrays.stream(fields).allMatch(f -> {
			if (f instanceof UnresolvedReferenceExpression) {
				return !inputNames.contains(((UnresolvedReferenceExpression) f).getName());
			}

			return true;
		});
	}

	/**
	 * Returns field names and field positions for a given {@link TypeInformation}.
	 *
	 * @param inputType The TypeInformation extract the field names and positions from.
	 * @param <A> The type of the TypeInformation.
	 * @return A tuple of two arrays holding the field names and corresponding field positions.
	 */
	public static <A> FieldsInfo getFieldInfo(TypeInformation<A> inputType) {

		if (inputType instanceof GenericTypeInfo && inputType.getTypeClass() == Row.class) {
			throw new TableException(
				"An input of GenericTypeInfo<Row> cannot be converted to Table. " +
					"Please specify the type of the input with a RowTypeInfo.");
		} else {
			return new FieldsInfo(getFieldNames(inputType), getFieldIndices(inputType));
		}
	}

	/**
	 * Returns field names and field positions for a given {@link TypeInformation} and array of
	 * {@link Expression}. It does not handle time attributes but considers them in indices.
	 *
	 * @param inputType The {@link TypeInformation} against which the {@link Expression}s are evaluated.
	 * @param exprs     The expressions that define the field names.
	 * @param <A> The type of the TypeInformation.
	 * @return A tuple of two arrays holding the field names and corresponding field positions.
	 */
	public static <A> FieldsInfo getFieldInfo(TypeInformation<A> inputType, Expression[] exprs) {
		validateType(inputType);

		final Set<FieldInfo> fieldInfos;
		if (inputType instanceof GenericTypeInfo && inputType.getTypeClass() == Row.class) {
			throw new TableException(
				"An input of GenericTypeInfo<Row> cannot be converted to Table. " +
					"Please specify the type of the input with a RowTypeInfo.");
		} else if (inputType instanceof TupleTypeInfoBase) {
			fieldInfos = extractFieldInfosFromTupleType((CompositeType) inputType, exprs);
		} else if (inputType instanceof PojoTypeInfo) {
			fieldInfos = extractFieldInfosByNameReference((CompositeType) inputType, exprs);
		} else {
			fieldInfos = extractFieldInfoFromAtomicType(exprs);
		}

		if (fieldInfos.stream().anyMatch(info -> info.getFieldName().equals("*"))) {
			throw new TableException("Field name can not be '*'.");
		}

		String[] fieldNames = fieldInfos.stream().map(FieldInfo::getFieldName).toArray(String[]::new);
		int[] fieldIndices = fieldInfos.stream().mapToInt(FieldInfo::getIndex).toArray();
		return new FieldsInfo(fieldNames, fieldIndices);
	}

	/**
	 * Returns field names for a given {@link TypeInformation}.
	 *
	 * @param inputType The TypeInformation extract the field names.
	 * @param <A> The type of the TypeInformation.
	 * @return An array holding the field names
	 */
	public static <A> String[] getFieldNames(TypeInformation<A> inputType) {
		validateType(inputType);

		final String[] fieldNames;
		if (inputType instanceof CompositeType) {
			fieldNames = ((CompositeType<A>) inputType).getFieldNames();
		} else {
			fieldNames = new String[]{"f0"};
		}

		if (Arrays.asList(fieldNames).contains("*")) {
			throw new TableException("Field name can not be '*'.");
		}

		return fieldNames;
	}

	/**
	 * Validate if class represented by the typeInfo is static and globally accessible.
	 *
	 * @param typeInfo type to check
	 * @throws TableException if type does not meet these criteria
	 */
	public static <A> void validateType(TypeInformation<A> typeInfo) {
		Class<A> clazz = typeInfo.getTypeClass();
		if ((clazz.isMemberClass() && !Modifier.isStatic(clazz.getModifiers())) ||
			!Modifier.isPublic(clazz.getModifiers()) ||
			clazz.getCanonicalName() == null) {
			throw new TableException(format(
				"Class '%s' described in type information '%s' must be " +
				"static and globally accessible.", clazz, typeInfo));
		}
	}

	/**
	 * Returns field indexes for a given {@link TypeInformation}.
	 *
	 * @param inputType The TypeInformation extract the field positions from.
	 * @return An array holding the field positions
	 */
	public static int[] getFieldIndices(TypeInformation<?> inputType) {
		return IntStream.range(0, getFieldNames(inputType).length).toArray();
	}

	/**
	 * Returns field types for a given {@link TypeInformation}.
	 *
	 * @param inputType The TypeInformation to extract field types from.
	 * @return An array holding the field types.
	 */
	public static TypeInformation<?>[] getFieldTypes(TypeInformation<?> inputType) {
		validateType(inputType);

		final TypeInformation<?>[] fieldTypes;
		if (inputType instanceof CompositeType) {
			int arity = inputType.getArity();
			CompositeType ct = (CompositeType) inputType;
			fieldTypes = IntStream.range(0, arity).mapToObj(ct::getTypeAt).toArray(TypeInformation[]::new);
		} else {
			fieldTypes = new TypeInformation[]{inputType};
		}

		return fieldTypes;
	}

	/* Utility methods */

	private static Set<FieldInfo> extractFieldInfoFromAtomicType(Expression[] exprs) {
		boolean referenced = false;
		FieldInfo fieldInfo = null;
		for (Expression expr : exprs) {
			if (expr instanceof UnresolvedReferenceExpression) {
				if (referenced) {
					throw new TableException("Only the first field can reference an atomic type.");
				} else {
					referenced = true;
					fieldInfo = new FieldInfo(((UnresolvedReferenceExpression) expr).getName(), 0);
				}
			} else if (!isTimeAttribute(expr)) { // IGNORE Time attributes
				throw new TableException("Field reference expression expected.");
			}
		}

		if (fieldInfo != null) {
			return Collections.singleton(fieldInfo);
		}

		return Collections.emptySet();
	}

	private static <A> Set<FieldInfo> extractFieldInfosByNameReference(CompositeType inputType, Expression[] exprs) {
		ExprToFieldInfo exprToFieldInfo = new ExprToFieldInfo(inputType);
		return Arrays.stream(exprs)
			.map(expr -> expr.accept(exprToFieldInfo))
			.filter(Optional::isPresent)
			.map(Optional::get)
			.collect(Collectors.toCollection(LinkedHashSet::new));
	}

	private static <A> Set<FieldInfo> extractFieldInfosFromTupleType(CompositeType inputType, Expression[] exprs) {
		boolean isRefByPos = isReferenceByPosition((CompositeType<?>) inputType, exprs);

		if (isRefByPos) {
			return IntStream.range(0, exprs.length)
				.mapToObj(idx -> exprs[idx].accept(new IndexedExprToFieldInfo(idx)))
				.filter(Optional::isPresent)
				.map(Optional::get)
				.collect(Collectors.toCollection(LinkedHashSet::new));
		} else {
			return extractFieldInfosByNameReference(inputType, exprs);
		}
	}

	private static class FieldInfo {
		private final String fieldName;
		private final int index;

		FieldInfo(String fieldName, int index) {
			this.fieldName = fieldName;
			this.index = index;
		}

		public String getFieldName() {
			return fieldName;
		}

		public int getIndex() {
			return index;
		}
	}

	private static class IndexedExprToFieldInfo extends ApiExpressionDefaultVisitor<Optional<FieldInfo>> {

		private final int index;

		private IndexedExprToFieldInfo(int index) {
			this.index = index;
		}

		@Override
		public Optional<FieldInfo> visitUnresolvedReference(UnresolvedReferenceExpression unresolvedReference) {
			String fieldName = unresolvedReference.getName();
			return Optional.of(new FieldInfo(fieldName, index));
		}

		@Override
		public Optional<FieldInfo> visitCall(CallExpression call) {
			if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.AS) {
				List<Expression> children = call.getChildren();
				Expression origExpr = children.get(0);
				String newName = ExpressionUtils.extractValue(children.get(1), Types.STRING)
					.orElseThrow(() ->
						new TableException("Alias expects string literal as new name. Got: " + children.get(1)));

				if (origExpr instanceof UnresolvedReferenceExpression) {
					throw new TableException(
						format("Alias '%s' is not allowed if other fields are referenced by position.", newName));
				} else if (isTimeAttribute(origExpr)) {
					return Optional.empty();
				}
			} else if (isTimeAttribute(call)) {
				return Optional.empty();
			}

			return defaultMethod(call);
		}

		@Override
		protected Optional<FieldInfo> defaultMethod(Expression expression) {
			throw new TableException("Field reference expression or alias on field expression expected.");
		}
	}

	private static class ExprToFieldInfo extends ApiExpressionDefaultVisitor<Optional<FieldInfo>> {

		private final CompositeType ct;

		private ExprToFieldInfo(CompositeType ct) {
			this.ct = ct;
		}

		@Override
		public Optional<FieldInfo> visitUnresolvedReference(UnresolvedReferenceExpression unresolvedReference) {
			String fieldName = unresolvedReference.getName();
			return referenceByName(fieldName, ct).map(idx -> new FieldInfo(fieldName, idx));
		}

		@Override
		public Optional<FieldInfo> visitCall(CallExpression call) {
			if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.AS) {
				List<Expression> children = call.getChildren();
				Expression origExpr = children.get(0);
				String newName = ExpressionUtils.extractValue(children.get(1), Types.STRING)
					.orElseThrow(() ->
						new TableException("Alias expects string literal as new name. Got: " + children.get(1)));

				if (origExpr instanceof UnresolvedReferenceExpression) {
					return referenceByName(((UnresolvedReferenceExpression) origExpr).getName(), ct)
						.map(idx -> new FieldInfo(newName, idx));
				} else if (isTimeAttribute(origExpr)) {
					return Optional.empty();
				}
			} else if (isTimeAttribute(call)) {
				return Optional.empty();
			}

			return defaultMethod(call);
		}

		@Override
		protected Optional<FieldInfo> defaultMethod(Expression expression) {
			throw new TableException("Field reference expression or alias on field expression expected.");
		}
	}

	private static boolean isTimeAttribute(Expression origExpr) {
		return origExpr instanceof CallExpression &&
			TIME_ATTRIBUTES.contains(((CallExpression) origExpr).getFunctionDefinition());
	}

	private static Optional<Integer> referenceByName(String name, CompositeType<?> ct) {
		int inputIdx = ct.getFieldIndex(name);
		if (inputIdx < 0) {
			throw new TableException(format(
				"%s is not a field of type %s. Expected: %s}",
				name,
				ct,
				String.join(", ", ct.getFieldNames())));
		} else {
			return Optional.of(inputIdx);
		}
	}

	private FieldInfoUtils() {
	}
}
