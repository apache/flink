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

package org.apache.flink.types.variant;

import org.apache.flink.annotation.PublicEvolving;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;

/** Variant represent a semi-structured data. */
@PublicEvolving
public interface Variant {

    /** Returns true if the variant is a primitive typed value, such as INT, DOUBLE, STRING, etc. */
    boolean isPrimitive();

    /** Returns true if this variant is an Array, false otherwise. */
    boolean isArray();

    /** Returns true if this variant is an Object, false otherwise. */
    boolean isObject();

    /** Check If this variant is null. */
    boolean isNull();

    /** Get the type of variant. */
    Type getType();

    /**
     * Get the scalar value of variant as boolean, if the variant type is {@link Type#BOOLEAN}.
     *
     * @throws VariantTypeException If this variant is not a scalar value or is not {@link
     *     Type#BOOLEAN}.
     */
    boolean getBoolean() throws VariantTypeException;

    /**
     * Get the scalar value of variant as byte, if the variant type is {@link Type#TINYINT}.
     *
     * @throws VariantTypeException If this variant is not a scalar value or is not {@link
     *     Type#TINYINT}.
     */
    byte getByte() throws VariantTypeException;

    /**
     * Get the scalar value of variant as short, if the variant type is {@link Type#SMALLINT}.
     *
     * @throws VariantTypeException If this variant is not a scalar value or is not {@link
     *     Type#SMALLINT}.
     */
    short getShort() throws VariantTypeException;

    /**
     * Get the scalar value of variant as int, if the variant type is {@link Type#INT}.
     *
     * @throws VariantTypeException if this variant is not a scalar value or is not {@link
     *     Type#INT}.
     */
    int getInt() throws VariantTypeException;

    /**
     * Get the scalar value of variant as long, if the variant type is {@link Type#BIGINT}.
     *
     * @throws VariantTypeException If this variant is not a scalar value or is not {@link
     *     Type#BIGINT}.
     */
    long getLong() throws VariantTypeException;

    /**
     * Get the scalar value of variant as float, if the variant type is {@link Type#FLOAT}.
     *
     * @throws VariantTypeException If this variant is not a scalar value or is not {@link
     *     Type#FLOAT}.
     */
    float getFloat() throws VariantTypeException;

    /**
     * Get the scalar value of variant as BigDecimal, if the variant type is {@link Type#DECIMAL}.
     *
     * @throws VariantTypeException If this variant is not a scalar value or is not {@link
     *     Type#DECIMAL}.
     */
    BigDecimal getDecimal() throws VariantTypeException;

    /**
     * Get the scalar value of variant as double, if the variant type is {@link Type#DOUBLE}.
     *
     * @throws VariantTypeException If this variant is not a scalar value or is not {@link
     *     Type#DOUBLE}.
     */
    double getDouble() throws VariantTypeException;

    /**
     * Get the scalar value of variant as string, if the variant type is {@link Type#STRING}.
     *
     * @throws VariantTypeException If this variant is not a scalar value or is not {@link
     *     Type#STRING}.
     */
    String getString() throws VariantTypeException;

    /**
     * Get the scalar value of variant as LocalDate, if the variant type is {@link Type#DATE}.
     *
     * @throws VariantTypeException If this variant is not a scalar value or is not {@link
     *     Type#DATE}.
     */
    LocalDate getDate() throws VariantTypeException;

    /**
     * Get the scalar value of variant as LocalDateTime, if the variant type is {@link
     * Type#TIMESTAMP}.
     *
     * @throws VariantTypeException If this variant is not a scalar value or is not {@link
     *     Type#TIMESTAMP}.
     */
    LocalDateTime getDateTime() throws VariantTypeException;

    /**
     * Get the scalar value of variant as Instant, if the variant type is {@link Type#TIMESTAMP}.
     *
     * @throws VariantTypeException If this variant is not a scalar value or is not {@link
     *     Type#TIMESTAMP}.
     */
    Instant getInstant() throws VariantTypeException;

    /**
     * Get the scalar value of variant as byte array, if the variant type is {@link Type#BYTES}.
     *
     * @throws VariantTypeException If this variant is not a scalar value or is not {@link
     *     Type#BYTES}.
     */
    byte[] getBytes() throws VariantTypeException;

    /**
     * Get the scalar value of variant.
     *
     * @throws VariantTypeException If this variant is not a scalar value.
     */
    Object get() throws VariantTypeException;

    /**
     * Get the scalar value of variant.
     *
     * @throws VariantTypeException If this variant is not a scalar value.
     */
    <T> T getAs() throws VariantTypeException;

    /**
     * Access value of the specified element of an array variant. If index is out of range, null is
     * returned.
     *
     * <p>NOTE: if the element value has been explicitly set as <code>null</code> (which is
     * different from removal!), a variant that @{@link Variant#isNull()} returns true will be
     * returned, not null.
     *
     * @throws VariantTypeException If this variant is not an array.
     */
    Variant getElement(int index) throws VariantTypeException;

    /**
     * Access value of the specified field of an object variant. If there is no field with the
     * specified name, null is returned.
     *
     * <p>NOTE: if the property value has been explicitly set as <code>null</code>, a variant
     * that @{@link Variant#isNull()} returns true will be returned, not null.
     *
     * @throws VariantTypeException If this variant is not an object.
     */
    Variant getField(String fieldName) throws VariantTypeException;

    /** Parses the variant to json. */
    String toJson();

    /** The type of variant. */
    @PublicEvolving
    enum Type {
        OBJECT,
        ARRAY,
        NULL,
        BOOLEAN,
        TINYINT,
        SMALLINT,
        INT,
        BIGINT,
        FLOAT,
        DOUBLE,
        DECIMAL,
        STRING,
        DATE,
        TIMESTAMP,
        TIMESTAMP_LTZ,
        BYTES
    }

    static VariantBuilder newBuilder() {
        return new BinaryVariantBuilder();
    }
}
