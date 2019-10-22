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

package org.apache.flink.table.sources;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.utils.DataTypeDefaultVisitor;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasFamily;

/**
 * Logic to validate {@link TableSource} types.
 */
@Internal
public class TableSourceValidation {

	/**
	 * Validates a TableSource.
	 *
	 * <ul>
	 * <li>checks that all fields of the schema can be resolved</li>
	 * <li>checks that resolved fields have the correct type</li>
	 * <li>checks that the time attributes are correctly configured.</li>
	 * </ul>
	 *
	 * @param tableSource The {@link TableSource} for which the time attributes are checked.
	 */
	public static void validateTableSource(TableSource<?> tableSource){
		TableSchema schema = tableSource.getTableSchema();

		List<RowtimeAttributeDescriptor> rowtimeAttributes = getRowtimeAttributes(tableSource);
		Optional<String> proctimeAttribute = getProctimeAttribute(tableSource);

		validateSingleRowtimeAttribute(rowtimeAttributes);
		validateRowtimeAttributesExistInSchema(rowtimeAttributes, schema);
		validateProctimeAttributesExistInSchema(proctimeAttribute, schema);
		validateLogicalToPhysicalMapping(tableSource, schema, rowtimeAttributes, proctimeAttribute);
		validateTimestampExtractorArguments(rowtimeAttributes, tableSource);
		validateNotOverlapping(rowtimeAttributes, proctimeAttribute);
	}

	/**
	 * Checks if the given {@link TableSource} defines a rowtime attribute.
	 *
	 * @param tableSource The table source to check.
	 * @return true if the given table source defines rotime attribute
	 */
	public static boolean hasRowtimeAttribute(TableSource<?> tableSource) {
		return !getRowtimeAttributes(tableSource).isEmpty();
	}

	private static void validateSingleRowtimeAttribute(List<RowtimeAttributeDescriptor> rowtimeAttributes) {
		if (rowtimeAttributes.size() > 1) {
			throw new ValidationException("Currently, only a single rowtime attribute is supported. " +
				"Please remove all but one RowtimeAttributeDescriptor.");
		}
	}

	private static void validateRowtimeAttributesExistInSchema(
			List<RowtimeAttributeDescriptor> rowtimeAttributes,
			TableSchema tableSchema) {
		rowtimeAttributes.forEach(r -> {
				if (!tableSchema.getFieldDataType(r.getAttributeName()).isPresent()) {
					throw new ValidationException(String.format(
						"Found a rowtime attribute for field '%s' but it does not exist in the Table. TableSchema: %s",
						r.getAttributeName(),
						tableSchema));
				}
			}
		);
	}

	private static void validateProctimeAttributesExistInSchema(
			Optional<String> proctimeAttribute,
			TableSchema tableSchema) {
		proctimeAttribute.ifPresent(r -> {
				if (!tableSchema.getFieldDataType(r).isPresent()) {
					throw new ValidationException(String.format(
						"Found a proctime attribute for field '%s' but it does not exist in the Table. TableSchema: %s",
						r,
						tableSchema));
				}
			}
		);
	}

	private static void validateNotOverlapping(
			List<RowtimeAttributeDescriptor> rowtimeAttributes,
			Optional<String> proctimeAttribute) {
		proctimeAttribute.ifPresent(proctime -> {
				if (rowtimeAttributes.stream()
					.anyMatch(rowtimeAttribute -> rowtimeAttribute.getAttributeName().equals(proctime))) {
					throw new ValidationException(String.format(
						"Field '%s' must not be processing time and rowtime attribute at the same time.",
						proctime));
				}
			}
		);
	}

	private static void validateLogicalToPhysicalMapping(
			TableSource<?> tableSource,
			TableSchema schema,
			List<RowtimeAttributeDescriptor> rowtimeAttributes,
			Optional<String> proctimeAttribute) {
		// validate that schema fields can be resolved to a return type field of correct type
		int mappedFieldCnt = 0;
		for (int i = 0; i < schema.getFieldCount(); i++) {
			DataType fieldType = schema.getFieldDataType(i).get();
			LogicalType logicalFieldType = fieldType.getLogicalType();
			String fieldName = schema.getFieldName(i).get();

			if (proctimeAttribute.map(p -> p.equals(fieldName)).orElse(false)) {
				if (!(hasFamily(logicalFieldType, LogicalTypeFamily.TIMESTAMP))) {
					throw new ValidationException(String.format("Processing time field '%s' has invalid type %s. " +
						"Processing time attributes must be of type SQL_TIMESTAMP.", fieldName, logicalFieldType));
				}
			} else if (rowtimeAttributes.stream().anyMatch(p -> p.getAttributeName().equals(fieldName))) {
				if (!(hasFamily(logicalFieldType, LogicalTypeFamily.TIMESTAMP))) {
					throw new ValidationException(String.format("Rowtime time field '%s' has invalid type %s. " +
						"Rowtime time attributes must be of type SQL_TIMESTAMP.", fieldName, logicalFieldType));
				}
			} else {
				validateLogicalTypeEqualsPhysical(fieldName, fieldType, tableSource);
				mappedFieldCnt += 1;
			}
		}

		// ensure that only one field is mapped to an atomic type
		DataType producedDataType = tableSource.getProducedDataType();
		if (!isCompositeType(producedDataType) && mappedFieldCnt > 1) {
			throw new ValidationException(
				String.format(
					"More than one table field matched to atomic input type %s.",
					producedDataType));
		}
	}

	private static boolean isCompositeType(DataType producedDataType) {
		LogicalType logicalType = producedDataType.getLogicalType();
		return producedDataType instanceof FieldsDataType ||
			(logicalType instanceof LegacyTypeInformationType &&
				((LegacyTypeInformationType) logicalType).getTypeInformation() instanceof CompositeType);
	}

	private static void validateLogicalTypeEqualsPhysical(
			String fieldName,
			DataType logicalType,
			TableSource<?> tableSource) {
		ResolvedField resolvedField = resolveField(fieldName, tableSource);
		if (!resolvedField.getType().equals(logicalType)) {
			throw new ValidationException(String.format(
				"Type %s of table field '%s' does not " +
					"match with type '%s; of the field '%s' of the TableSource return type.",
				logicalType,
				resolvedField.getType(),
				fieldName,
				resolvedField.getType()));
		}
	}

	private static void validateTimestampExtractorArguments(
			List<RowtimeAttributeDescriptor> descriptors,
			TableSource<?> tableSource) {
		if (descriptors.size() == 1) {
			RowtimeAttributeDescriptor descriptor = descriptors.get(0);
			// look up extractor input fields in return type
			String[] extractorInputFields = descriptor.getTimestampExtractor().getArgumentFields();
			TypeInformation[] physicalTypes = Arrays.stream(extractorInputFields)
				.map(fieldName -> resolveField(fieldName, tableSource))
				.map(resolvedField -> TypeConversions.fromDataTypeToLegacyInfo(resolvedField.getType()))
				.toArray(TypeInformation[]::new);
			// validate timestamp extractor
			descriptor.getTimestampExtractor().validateArgumentFields(physicalTypes);
		}
	}

	private static class ResolvedField {
		private final String name;
		private final DataType type;

		private ResolvedField(String name, DataType type) {
			this.type = type;
			this.name = name;
		}

		public DataType getType() {
			return type;
		}

		public String getName() {
			return name;
		}
	}

	/**
	 * Identifies for a field name of the logical schema, the corresponding physical field in the
	 * return type of a {@link TableSource}.
	 *
	 * @param fieldName The logical field to look up.
	 * @param tableSource The table source in which to look for the field.
	 * @return The name, index, and type information of the physical field.
	 */
	private static ResolvedField resolveField(String fieldName, TableSource<?> tableSource) {

		DataType producedDataType = tableSource.getProducedDataType();

		if (tableSource instanceof DefinedFieldMapping) {
			Map<String, String> fieldMapping = ((DefinedFieldMapping) tableSource).getFieldMapping();
			if (fieldMapping != null) {
				String resolvedFieldName = fieldMapping.get(fieldName);
				if (resolvedFieldName == null) {
					throw new ValidationException(String.format(
						"Field '%s' could not be resolved by the field mapping.",
						fieldName));
				}

				return new ResolvedField(
					resolvedFieldName,
					lookupFieldType(
						producedDataType,
						resolvedFieldName,
						String.format(
							"Table field '%s' was resolved to TableSource return type field " +
								"'%s', but field '%s' was not found in the return " +
								"type %s of the TableSource. " +
								"Please verify the field mapping of the TableSource.",
							fieldName,
							resolvedFieldName,
							resolvedFieldName,
							producedDataType)));
			}
		}

		return new ResolvedField(
			fieldName,
			lookupFieldType(
				producedDataType,
				fieldName,
				String.format(
					"Table field '%s' was not found in the return type %s of the TableSource.",
					fieldName,
					producedDataType)));
	}

	/** Look up a field by name in a {@link DataType}. */
	private static DataType lookupFieldType(DataType inputType, String fieldName, String failMsg) {
		return inputType.accept(new TypeExtractor(fieldName)).orElseThrow(() -> new ValidationException(failMsg));
	}

	private static class TypeExtractor extends DataTypeDefaultVisitor<Optional<DataType>> {
		private final String fieldName;

		TypeExtractor(String fieldName) {
			this.fieldName = fieldName;
		}

		@Override
		public Optional<DataType> visit(AtomicDataType atomicDataType) {
			//  This is check for backwards compatibility. We should also support legacy type with composite type info
			LogicalType logicalType = atomicDataType.getLogicalType();
			if (logicalType instanceof LegacyTypeInformationType) {
				LegacyTypeInformationType<?> legacyTypeInformationType = (LegacyTypeInformationType<?>) logicalType;
				TypeInformation<?> typeInformation = legacyTypeInformationType.getTypeInformation();
				if (typeInformation instanceof CompositeType<?>) {
					CompositeType<?> compositeType = (CompositeType<?>) typeInformation;
					return Optional.of(TypeConversions.fromLegacyInfoToDataType(compositeType.getTypeAt(fieldName)));
				}
			}

			return Optional.of(atomicDataType);
		}

		@Override
		public Optional<DataType> visit(FieldsDataType fieldsDataType) {
			return Optional.ofNullable(fieldsDataType.getFieldDataTypes().get(fieldName));
		}

		@Override
		protected Optional<DataType> defaultMethod(DataType dataType) {
			return Optional.of(dataType);
		}
	}

	/** Returns a list with all rowtime attribute descriptors of the {@link TableSource}. */
	private static List<RowtimeAttributeDescriptor> getRowtimeAttributes(TableSource<?> tableSource) {
		if (tableSource instanceof DefinedRowtimeAttributes) {
			return ((DefinedRowtimeAttributes) tableSource).getRowtimeAttributeDescriptors();
		}

		return Collections.emptyList();
	}

	/** Returns the proctime attribute of the {@link TableSource} if it is defined. */
	private static Optional<String> getProctimeAttribute(TableSource<?> tableSource) {
		if (tableSource instanceof DefinedProctimeAttribute) {
			return Optional.ofNullable(((DefinedProctimeAttribute) tableSource).getProctimeAttribute());
		}

		return Optional.empty();
	}

	private TableSourceValidation() {
	}
}
