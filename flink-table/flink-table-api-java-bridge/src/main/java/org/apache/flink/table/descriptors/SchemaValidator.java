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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.TableFormatFactory;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.tsextractors.TimestampExtractor;
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * Validator for {@link Schema}.
 */
@PublicEvolving
public class SchemaValidator implements DescriptorValidator {

	private final boolean isStreamEnvironment;
	private final boolean supportsSourceTimestamps;
	private final boolean supportsSourceWatermarks;

	public SchemaValidator(boolean isStreamEnvironment, boolean supportsSourceTimestamps,
			boolean supportsSourceWatermarks) {
		this.isStreamEnvironment = isStreamEnvironment;
		this.supportsSourceTimestamps = supportsSourceTimestamps;
		this.supportsSourceWatermarks = supportsSourceWatermarks;
	}

	@Override
	public void validate(DescriptorProperties properties) {
		Map<String, String> names = properties.getIndexedProperty(SCHEMA, SCHEMA_NAME);
		Map<String, String> types = properties.getIndexedProperty(SCHEMA, SCHEMA_TYPE);

		if (names.isEmpty() && types.isEmpty()) {
			throw new ValidationException(
					format("Could not find the required schema in property '%s'.", SCHEMA));
		}

		boolean proctimeFound = false;

		for (int i = 0; i < Math.max(names.size(), types.size()); i++) {
			properties.validateString(SCHEMA + "." + i + "." + SCHEMA_NAME, false, 1);
			properties.validateType(SCHEMA + "." + i + "." + SCHEMA_TYPE, false, false);
			properties.validateString(SCHEMA + "." + i + "." + SCHEMA_FROM, true, 1);
			// either proctime or rowtime
			String proctime = SCHEMA + "." + i + "." + SCHEMA_PROCTIME;
			String rowtime = SCHEMA + "." + i + "." + ROWTIME;
			if (properties.containsKey(proctime)) {
				// check the environment
				if (!isStreamEnvironment) {
					throw new ValidationException(
							format("Property '%s' is not allowed in a batch environment.", proctime));
				}
				// check for only one proctime attribute
				else if (proctimeFound) {
					throw new ValidationException("A proctime attribute must only be defined once.");
				}
				// check proctime
				properties.validateBoolean(proctime, false);
				proctimeFound = properties.getBoolean(proctime);
				// no rowtime
				properties.validatePrefixExclusion(rowtime);
			} else if (properties.hasPrefix(rowtime)) {
				// check rowtime
				RowtimeValidator rowtimeValidator = new RowtimeValidator(
						supportsSourceTimestamps,
						supportsSourceWatermarks,
						SCHEMA + "." + i + ".");
				rowtimeValidator.validate(properties);
				// no proctime
				properties.validateExclusion(proctime);
			}
		}
	}

	/**
	 * Returns keys for a {@link TableFormatFactory#supportedProperties()} method that
	 * are accepted for schema derivation using {@code deriveFormatFields(DescriptorProperties)}.
	 */
	public static List<String> getSchemaDerivationKeys() {
		List<String> keys = new ArrayList<>();

		// schema
		keys.add(SCHEMA + ".#." + SCHEMA_TYPE);
		keys.add(SCHEMA + ".#." + SCHEMA_NAME);
		keys.add(SCHEMA + ".#." + SCHEMA_FROM);

		// time attributes
		keys.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
		keys.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
		keys.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
		keys.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
		keys.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
		keys.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
		keys.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
		keys.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
		keys.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);

		return keys;
	}

	/**
	 * Finds the proctime attribute if defined.
	 */
	public static Optional<String> deriveProctimeAttribute(DescriptorProperties properties) {
		Map<String, String> names = properties.getIndexedProperty(SCHEMA, SCHEMA_NAME);

		for (int i = 0; i < names.size(); i++) {
			Optional<Boolean> isProctime = properties.getOptionalBoolean(SCHEMA + "." + i + "." + SCHEMA_PROCTIME);
			if (isProctime.isPresent() && isProctime.get()) {
				return Optional.of(names.get(SCHEMA + "." + i + "." + SCHEMA_NAME));
			}
		}
		return Optional.empty();
	}

	/**
	 * Finds the rowtime attributes if defined.
	 */
	public static List<RowtimeAttributeDescriptor> deriveRowtimeAttributes(
			DescriptorProperties properties) {

		Map<String, String> names = properties.getIndexedProperty(SCHEMA, SCHEMA_NAME);

		List<RowtimeAttributeDescriptor> attributes = new ArrayList<>();

		// check for rowtime in every field
		for (int i = 0; i < names.size(); i++) {
			Optional<Tuple2<TimestampExtractor, WatermarkStrategy>> rowtimeComponents = RowtimeValidator
					.getRowtimeComponents(properties, SCHEMA + "." + i + ".");
			int index = i;
			// create descriptor
			rowtimeComponents.ifPresent(tuple2 -> attributes.add(new RowtimeAttributeDescriptor(
					properties.getString(SCHEMA + "." + index + "." + SCHEMA_NAME),
					tuple2.f0,
					tuple2.f1))
			);
		}

		return attributes;
	}

	/**
	 * Derives the table schema for a table sink. A sink ignores a proctime attribute and
	 * needs to track the origin of a rowtime field.
	 *
	 * @deprecated This method combines two separate concepts of table schema and field mapping.
	 *             This should be split into two methods once we have support for
	 *             the corresponding interfaces (see FLINK-9870).
	 */
	@Deprecated
	public static TableSchema deriveTableSinkSchema(DescriptorProperties properties) {
		TableSchema.Builder builder = TableSchema.builder();

		TableSchema schema = properties.getTableSchema(SCHEMA);

		for (int i = 0; i < schema.getFieldCount(); i++) {
			TypeInformation t = schema.getFieldTypes()[i];
			String n = schema.getFieldNames()[i];
			boolean isProctime = properties
					.getOptionalBoolean(SCHEMA + "." + i + "." + SCHEMA_PROCTIME)
					.orElse(false);
			String tsType = SCHEMA + "." + i + "." + ROWTIME_TIMESTAMPS_TYPE;
			boolean isRowtime = properties.containsKey(tsType);
			if (!isProctime && !isRowtime) {
				// check for a aliasing
				String fieldName = properties.getOptionalString(SCHEMA + "." + i + "." + SCHEMA_FROM)
						.orElse(n);
				builder.field(fieldName, t);
			}
			// only use the rowtime attribute if it references a field
			else if (isRowtime) {
				switch (properties.getString(tsType)) {
					case ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD:
						String field = properties.getString(SCHEMA + "." + i + "." + ROWTIME_TIMESTAMPS_FROM);
						builder.field(field, t);
						break;
					// other timestamp strategies require a reverse timestamp extractor to
					// insert the timestamp into the output
					default:
						throw new TableException(format("Unsupported rowtime type '%s' for sink" +
								" table schema. Currently only '%s' is supported for table sinks.",
								t, ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD));
				}
			}
		}

		return builder.build();
	}

	/**
	 * Finds a table source field mapping.
	 *
	 * @param properties The properties describing a schema.
	 * @param inputType  The input type that a connector and/or format produces. This parameter
	 *                   can be used to resolve a rowtime field against an input field.
	 */
	public static Map<String, String> deriveFieldMapping(
			DescriptorProperties properties, Optional<TypeInformation<?>> inputType) {

		Map<String, String> mapping = new HashMap<>();

		TableSchema schema = properties.getTableSchema(SCHEMA);

		List<String> columnNames = new ArrayList<>();
		inputType.ifPresent(t -> columnNames.addAll(Arrays.asList(((CompositeType) t).getFieldNames())));

		// add all source fields first because rowtime might reference one of them
		columnNames.forEach(name -> mapping.put(name, name));

		// add all schema fields first for implicit mappings
		Arrays.stream(schema.getFieldNames()).forEach(name -> mapping.put(name, name));

		Map<String, String> names = properties.getIndexedProperty(SCHEMA, SCHEMA_NAME);

		for (int i = 0; i < names.size(); i++) {
			String name = properties.getString(SCHEMA + "." + i + "." + SCHEMA_NAME);
			Optional<String> source = properties.getOptionalString(SCHEMA + "." + i + "." + SCHEMA_FROM);
			if (source.isPresent()) {
				// add explicit mapping
				mapping.put(name, source.get());
			} else { // implicit mapping or time
				boolean isProctime = properties
						.getOptionalBoolean(SCHEMA + "." + i + "." + SCHEMA_PROCTIME)
						.orElse(false);
				boolean isRowtime = properties
						.containsKey(SCHEMA + "." + i + "." + ROWTIME_TIMESTAMPS_TYPE);
				// remove proctime/rowtime from mapping
				if (isProctime || isRowtime) {
					mapping.remove(name);
				}
				// check for invalid fields
				else if (!columnNames.contains(name)) {
					throw new ValidationException(format("Could not map the schema field '%s' to a field " +
							"from source. Please specify the source field from which it can be derived.", name));
				}
			}
		}

		return mapping;
	}
}
