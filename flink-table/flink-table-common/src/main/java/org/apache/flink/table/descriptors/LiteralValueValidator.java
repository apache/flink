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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

/** Validator for {@link LiteralValue}. */
@Internal
public class LiteralValueValidator extends HierarchyDescriptorValidator {

    public static final String TYPE = "type";
    public static final String VALUE = "value";

    /*
     * TODO The following types need to be supported next.
     * Types.SQL_DATE
     * Types.SQL_TIME
     * Types.SQL_TIMESTAMP
     * Types.PRIMITIVE_ARRAY
     * Types.OBJECT_ARRAY
     * Types.MAP
     * Types.MULTISET
     * null
     */

    /**
     * Gets the value according to the type and value strings.
     *
     * @param keyPrefix the prefix of the literal type key
     * @param properties the descriptor properties
     * @return the derived value
     */
    public static Object getValue(String keyPrefix, DescriptorProperties properties) {
        String typeKey = keyPrefix + TYPE;
        // explicit type
        if (properties.containsKey(typeKey)) {
            String valueKey = keyPrefix + VALUE;
            TypeInformation<?> typeInfo = properties.getType(typeKey);

            if (typeInfo.equals(Types.BIG_DEC)) {
                return properties.getBigDecimal(valueKey);
            } else if (typeInfo.equals(Types.BOOLEAN)) {
                return properties.getBoolean(valueKey);
            } else if (typeInfo.equals(Types.BYTE)) {
                return properties.getByte(valueKey);
            } else if (typeInfo.equals(Types.DOUBLE)) {
                return properties.getDouble(valueKey);
            } else if (typeInfo.equals(Types.FLOAT)) {
                return properties.getFloat(valueKey);
            } else if (typeInfo.equals(Types.INT)) {
                return properties.getInt(valueKey);
            } else if (typeInfo.equals(Types.LONG)) {
                return properties.getLong(valueKey);
            } else if (typeInfo.equals(Types.SHORT)) {
                return properties.getShort(valueKey);
            } else if (typeInfo.equals(Types.STRING)) {
                return properties.getString(valueKey);
            } else {
                throw new TableException("Unsupported type '" + typeInfo.getTypeClass() + "'.");
            }
        }
        // implicit type
        else {
            return deriveTypeStringFromValueString(
                    properties.getString(keyPrefix.substring(0, keyPrefix.length() - 1)));
        }
    }

    /**
     * Tries to derive a literal value from the given string value. The derivation priority for the
     * types are BOOLEAN, INT, DOUBLE, and VARCHAR.
     *
     * @param valueString the string formatted value
     * @return parsed value
     */
    private static Object deriveTypeStringFromValueString(String valueString) {
        if (valueString.equals("true") || valueString.equals("false")) {
            return Boolean.valueOf(valueString);
        } else {
            try {
                return Integer.valueOf(valueString);
            } catch (NumberFormatException e1) {
                try {
                    return Double.valueOf(valueString);
                } catch (NumberFormatException e2) {
                    return valueString;
                }
            }
        }
    }

    /** @param keyPrefix prefix to be added to every property before validation */
    public LiteralValueValidator(String keyPrefix) {
        super(keyPrefix);
    }

    @Override
    protected void validateWithPrefix(String keyPrefix, DescriptorProperties properties) {
        String typeKey = keyPrefix + TYPE;
        properties.validateType(typeKey, true, false);

        // explicit type
        if (properties.containsKey(typeKey)) {
            String valueKey = keyPrefix + VALUE;
            TypeInformation<?> typeInfo = properties.getType(typeKey);
            if (typeInfo.equals(Types.BIG_DEC)) {
                properties.validateBigDecimal(valueKey, false);
            } else if (typeInfo.equals(Types.BOOLEAN)) {
                properties.validateBoolean(valueKey, false);
            } else if (typeInfo.equals(Types.BYTE)) {
                properties.validateByte(valueKey, false);
            } else if (typeInfo.equals(Types.DOUBLE)) {
                properties.validateDouble(valueKey, false);
            } else if (typeInfo.equals(Types.FLOAT)) {
                properties.validateFloat(valueKey, false);
            } else if (typeInfo.equals(Types.INT)) {
                properties.validateInt(valueKey, false);
            } else if (typeInfo.equals(Types.LONG)) {
                properties.validateLong(valueKey, false);
            } else if (typeInfo.equals(Types.SHORT)) {
                properties.validateShort(valueKey, false);
            } else if (typeInfo.equals(Types.STRING)) {
                properties.validateString(valueKey, false);
            } else {
                throw new TableException("Unsupported type '" + typeInfo + "'.");
            }
        }
        // implicit type
        else {
            // do not allow values in top-level
            if (keyPrefix.equals(HierarchyDescriptorValidator.EMPTY_PREFIX)) {
                throw new ValidationException(
                        "Literal values with implicit type must not exist in the top level of a hierarchy.");
            }
            properties.validateString(keyPrefix.substring(0, keyPrefix.length() - 1), false);
        }
    }
}
