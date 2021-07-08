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

package org.apache.flink.table.types;

import org.apache.flink.table.types.logical.LogicalType;

import org.hamcrest.CoreMatchers;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

/** Utilities for testing types. */
public class TypeTestingUtils {

    public static Matcher<DataType> hasLogicalType(LogicalType logicalType) {
        return new FeatureMatcher<DataType, LogicalType>(
                CoreMatchers.equalTo(logicalType),
                "logical type of the data type",
                "logical type") {

            @Override
            protected LogicalType featureValueOf(DataType actual) {
                return actual.getLogicalType();
            }
        };
    }

    public static Matcher<DataType> hasConversionClass(Class<?> clazz) {
        return new FeatureMatcher<DataType, Class<?>>(
                CoreMatchers.equalTo(clazz),
                "conversion class of the data type",
                "conversion class") {

            @Override
            protected Class<?> featureValueOf(DataType actual) {
                return actual.getConversionClass();
            }
        };
    }

    public static Matcher<DataType> hasNullability(boolean isNullable) {
        return new FeatureMatcher<DataType, Boolean>(
                CoreMatchers.equalTo(isNullable), "nullability of the data type", "nullability") {

            @Override
            protected Boolean featureValueOf(DataType actual) {
                return actual.getLogicalType().isNullable();
            }
        };
    }
}
