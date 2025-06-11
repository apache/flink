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

/** Builder for variants. */
@PublicEvolving
public interface VariantBuilder {

    /** Create a variant from a byte. */
    Variant of(byte b);

    /** Create a variant from a short. */
    Variant of(short s);

    /** Create a variant from a int. */
    Variant of(int i);

    /** Create a variant from a long. */
    Variant of(long l);

    /** Create a variant from a string. */
    Variant of(String s);

    /** Create a variant from a double. */
    Variant of(double d);

    /** Create a variant from a float. */
    Variant of(float f);

    /** Create a variant from a byte array. */
    Variant of(byte[] bytes);

    /** Create a variant from a boolean. */
    Variant of(boolean b);

    /** Create a variant from a BigDecimal. */
    Variant of(BigDecimal bigDecimal);

    /** Create a variant from an Instant. */
    Variant of(Instant instant);

    /** Create a variant from a LocalDate. */
    Variant of(LocalDate localDate);

    /** Create a variant from a LocalDateTime. */
    Variant of(LocalDateTime localDateTime);

    /** Create a variant of null. */
    Variant ofNull();

    /** Get the builder of a variant object. */
    VariantObjectBuilder object();

    /** Get the builder of a variant object. */
    VariantObjectBuilder object(boolean allowDuplicateKeys);

    /** Get the builder for a variant array. */
    VariantArrayBuilder array();

    /** Builder for a variant object. */
    @PublicEvolving
    interface VariantObjectBuilder {

        /** Add a field to the object. */
        VariantObjectBuilder add(String key, Variant value);

        /** Build the variant object. */
        Variant build();
    }

    /** Builder for a variant array. */
    @PublicEvolving
    interface VariantArrayBuilder {

        /** Add a value to the array. */
        VariantArrayBuilder add(Variant value);

        /** Build the variant array. */
        Variant build();
    }
}
