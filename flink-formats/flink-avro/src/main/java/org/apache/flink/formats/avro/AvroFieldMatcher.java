/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Resolves {@link FieldMatching#NAME} pairings between the fields of a Flink {@link RowType} and
 * the fields of an Avro record {@link Schema}.
 *
 * <p>Matching is attempted in three stages, so that the more surprising rules can only ever apply
 * to fields the stricter rules left over:
 *
 * <ol>
 *   <li>an exact, case-sensitive comparison of the Avro field name;
 *   <li>an exact, case-sensitive comparison against the Avro field <i>aliases</i>;
 *   <li>a case-insensitive ({@link Locale#ROOT}) comparison against names and aliases.
 * </ol>
 *
 * <p>Anything that cannot be resolved unambiguously, and anything that would silently lose or
 * fabricate data, is rejected with a message that names the offending field. In particular a column
 * that has no Avro counterpart is an error when writing (the column would be dropped) and an error
 * when reading if the column is {@code NOT NULL} (the column could only be read as {@code NULL}).
 * Avro fields that no column maps to are tolerated: when writing they must be nullable or declare a
 * default, when reading they are simply ignored.
 *
 * <p>Resolution is deliberately expensive and done once per (row type, schema) pair; the resulting
 * {@link Plan} reduces the per-record cost to an array lookup.
 */
@Internal
public final class AvroFieldMatcher {

    /** Returned by {@link Plan#avroPositionOf(int)} for a column with no Avro counterpart. */
    public static final int UNMATCHED = -1;

    /** Internal marker for a lookup key that would match more than one Avro field. */
    private static final int AMBIGUOUS = -2;

    private AvroFieldMatcher() {}

    /**
     * Resolves the pairing used to convert a {@link RowType} into a record of the given schema.
     *
     * @throws IllegalArgumentException if the pairing is ambiguous, or if it would drop a column,
     *     or if it would leave an Avro field that is neither nullable nor defaulted unwritten.
     */
    public static Plan forSerialization(RowType rowType, Schema recordSchema) {
        return resolve(rowType, recordSchema, true);
    }

    /**
     * Resolves the pairing used to convert a record of the given schema into a {@link RowType}.
     *
     * @throws IllegalArgumentException if the pairing is ambiguous, or if a {@code NOT NULL} column
     *     has no Avro counterpart.
     */
    public static Plan forDeserialization(RowType rowType, Schema recordSchema) {
        return resolve(rowType, recordSchema, false);
    }

    private static Plan resolve(RowType rowType, Schema recordSchema, boolean forSerialization) {
        if (recordSchema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException(
                    String.format(
                            "Matching fields by name requires an Avro RECORD schema for row type %s, but got: %s",
                            rowType, recordSchema));
        }

        final List<Schema.Field> avroFields = recordSchema.getFields();
        final List<String> rowFieldNames = rowType.getFieldNames();
        final int arity = rowFieldNames.size();

        final int[] rowToAvroPos = new int[arity];
        final int[] avroPosToRowField = new int[avroFields.size()];
        Arrays.fill(rowToAvroPos, UNMATCHED);
        Arrays.fill(avroPosToRowField, UNMATCHED);

        // Stage 1: exact names. Row field names are unique, so this stage cannot conflict with
        // itself, and running it to completion first guarantees that a fuzzy match can never
        // claim an Avro field that some other column matches exactly.
        for (int i = 0; i < arity; i++) {
            final Schema.Field avroField = recordSchema.getField(rowFieldNames.get(i));
            if (avroField != null) {
                rowToAvroPos[i] = avroField.pos();
                avroPosToRowField[avroField.pos()] = i;
            }
        }

        // Stages 2 and 3: aliases, then a case-insensitive comparison.
        final Map<String, Integer> byAlias = new HashMap<>();
        final Map<String, Integer> byLowerCase = new HashMap<>();
        for (Schema.Field avroField : avroFields) {
            index(byLowerCase, avroField.name(), avroField.pos());
            for (String alias : avroField.aliases()) {
                byAlias.merge(alias, avroField.pos(), AvroFieldMatcher::mergePositions);
                index(byLowerCase, alias, avroField.pos());
            }
        }

        for (int i = 0; i < arity; i++) {
            if (rowToAvroPos[i] != UNMATCHED) {
                continue;
            }
            final String rowFieldName = rowFieldNames.get(i);

            String rule = "an Avro field alias";
            int candidate = byAlias.getOrDefault(rowFieldName, UNMATCHED);
            if (candidate == UNMATCHED) {
                rule = "a case-insensitive comparison";
                candidate =
                        byLowerCase.getOrDefault(rowFieldName.toLowerCase(Locale.ROOT), UNMATCHED);
            }

            if (candidate == UNMATCHED) {
                continue;
            }
            if (candidate == AMBIGUOUS) {
                throw new IllegalArgumentException(
                        String.format(
                                "Column '%s' matches more than one field of the Avro record '%s' by %s. "
                                        + "Rename the column so that it matches exactly one Avro field "
                                        + "(fields: %s).",
                                rowFieldName,
                                recordSchema.getFullName(),
                                rule,
                                fieldNames(avroFields)));
            }
            final int competitor = avroPosToRowField[candidate];
            if (competitor != UNMATCHED) {
                throw new IllegalArgumentException(
                        String.format(
                                "Columns '%s' and '%s' both match field '%s' of the Avro record '%s'. "
                                        + "Rename one of the columns so that every Avro field is claimed "
                                        + "at most once.",
                                rowFieldNames.get(competitor),
                                rowFieldName,
                                avroFields.get(candidate).name(),
                                recordSchema.getFullName()));
            }

            rowToAvroPos[i] = candidate;
            avroPosToRowField[candidate] = i;
        }

        return forSerialization
                ? serializationPlan(recordSchema, rowToAvroPos, avroPosToRowField, rowFieldNames)
                : deserializationPlan(rowType, recordSchema, rowToAvroPos, rowFieldNames);
    }

    private static Plan serializationPlan(
            Schema recordSchema,
            int[] rowToAvroPos,
            int[] avroPosToRowField,
            List<String> rowFieldNames) {
        final List<Schema.Field> avroFields = recordSchema.getFields();

        // A column with no Avro counterpart would be dropped without a trace.
        for (int i = 0; i < rowToAvroPos.length; i++) {
            if (rowToAvroPos[i] == UNMATCHED) {
                throw new IllegalArgumentException(
                        String.format(
                                "Column '%s' cannot be written: the Avro record '%s' has no field "
                                        + "matching that name (fields: %s). Add the field to the Avro "
                                        + "schema, or project the column away before writing.",
                                rowFieldNames.get(i),
                                recordSchema.getFullName(),
                                fieldNames(avroFields)));
            }
        }

        // Avro fields that nothing writes to have to be either nullable or defaulted, otherwise
        // the record cannot be encoded at all and Avro reports it as an opaque
        // NullPointerException at the first record.
        final List<Integer> defaultedPositions = new ArrayList<>(0);
        final List<Object> defaultValues = new ArrayList<>(0);
        for (Schema.Field avroField : avroFields) {
            if (avroPosToRowField[avroField.pos()] != UNMATCHED) {
                continue;
            }
            if (avroField.hasDefaultValue()) {
                final Object defaultValue = materializeDefault(recordSchema, avroField);
                // A null default needs no action: a fresh record is null everywhere.
                if (defaultValue != null) {
                    defaultedPositions.add(avroField.pos());
                    defaultValues.add(defaultValue);
                }
            } else if (!isNullable(avroField.schema())) {
                throw new IllegalArgumentException(
                        String.format(
                                "Field '%s' of the Avro record '%s' is neither nullable nor does it "
                                        + "declare a default value, but no column matches it "
                                        + "(columns: %s). Add a matching column, or make the Avro field "
                                        + "nullable, or give it a default value.",
                                avroField.name(),
                                recordSchema.getFullName(),
                                String.join(", ", rowFieldNames)));
            }
        }

        return new Plan(
                recordSchema,
                rowToAvroPos,
                defaultedPositions.stream().mapToInt(Integer::intValue).toArray(),
                defaultValues.toArray());
    }

    private static Plan deserializationPlan(
            RowType rowType, Schema recordSchema, int[] rowToAvroPos, List<String> rowFieldNames) {
        // A NOT NULL column with no Avro counterpart could only ever be read as null, which
        // silently violates the contract the rest of the plan relies on.
        for (int i = 0; i < rowToAvroPos.length; i++) {
            if (rowToAvroPos[i] == UNMATCHED && !rowType.getTypeAt(i).isNullable()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Column '%s' is declared NOT NULL, but the Avro record '%s' has no "
                                        + "field matching that name (fields: %s), so it could only be "
                                        + "read as NULL. Add the field to the Avro schema, or make the "
                                        + "column nullable.",
                                rowFieldNames.get(i),
                                recordSchema.getFullName(),
                                fieldNames(recordSchema.getFields())));
            }
        }
        return new Plan(recordSchema, rowToAvroPos, new int[0], new Object[0]);
    }

    private static void index(Map<String, Integer> lookup, String name, int position) {
        lookup.merge(name.toLowerCase(Locale.ROOT), position, AvroFieldMatcher::mergePositions);
    }

    private static int mergePositions(int existing, int added) {
        return existing == added ? existing : AMBIGUOUS;
    }

    private static Object materializeDefault(Schema recordSchema, Schema.Field avroField) {
        final GenericData genericData = GenericData.get();
        try {
            // Copy the value: GenericData caches one shared instance of every default, and for
            // records, arrays and maps that instance would otherwise be aliased by every record
            // this converter produces.
            return genericData.deepCopy(avroField.schema(), genericData.getDefaultValue(avroField));
        } catch (AvroRuntimeException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot use the default value of field '%s' of the Avro record '%s', "
                                    + "which is required because no column matches that field.",
                            avroField.name(), recordSchema.getFullName()),
                    e);
        }
    }

    private static boolean isNullable(Schema schema) {
        if (schema.getType() == Schema.Type.NULL) {
            return true;
        }
        return schema.getType() == Schema.Type.UNION
                && schema.getTypes().stream().anyMatch(t -> t.getType() == Schema.Type.NULL);
    }

    private static String fieldNames(List<Schema.Field> avroFields) {
        return avroFields.stream().map(Schema.Field::name).collect(Collectors.joining(", "));
    }

    /**
     * An immutable, resolved pairing between a {@link RowType} and one Avro record {@link Schema}.
     *
     * <p>Every field is {@code final} and no reachable state is mutated after construction, so an
     * instance may be published through a plain, non-volatile field: the JMM guarantees that a
     * thread which observes the reference also observes the arrays it was built with.
     */
    public static final class Plan {

        private final Schema recordSchema;
        private final int[] rowToAvroPos;
        private final int[] defaultedAvroPos;
        private final Object[] defaultValues;

        private Plan(
                Schema recordSchema,
                int[] rowToAvroPos,
                int[] defaultedAvroPos,
                Object[] defaultValues) {
            this.recordSchema = recordSchema;
            this.rowToAvroPos = rowToAvroPos;
            this.defaultedAvroPos = defaultedAvroPos;
            this.defaultValues = defaultValues;
        }

        /**
         * Whether this plan was resolved against exactly the given schema instance.
         *
         * <p>Compares by identity on purpose: it is a cache guard, and Avro's {@code equals} walks
         * the whole schema. A miss only costs a re-resolution, and in practice the same instance is
         * handed to a converter for its entire lifetime.
         */
        public boolean appliesTo(Schema schema) {
            return recordSchema == schema;
        }

        /**
         * The position of the Avro field paired with the given row field, or {@link #UNMATCHED} if
         * the row field has no counterpart.
         */
        public int avroPositionOf(int rowFieldIndex) {
            return rowToAvroPos[rowFieldIndex];
        }

        /**
         * Writes the declared default of every Avro field that no row field maps to. Must be called
         * once per freshly created record; usually a no-op.
         */
        public void fillDefaults(IndexedRecord record) {
            for (int i = 0; i < defaultedAvroPos.length; i++) {
                record.put(defaultedAvroPos[i], defaultValues[i]);
            }
        }
    }
}
