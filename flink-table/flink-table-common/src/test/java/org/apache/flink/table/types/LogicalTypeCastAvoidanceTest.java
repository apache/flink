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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for {@link LogicalTypeCasts#supportsAvoidingCast(LogicalType, LogicalType)}. */
@RunWith(Parameterized.class)
public class LogicalTypeCastAvoidanceTest {

    @Parameters(name = "{index}: [{0} COMPATIBLE {1} => {2}")
    public static List<Object[]> testData() {
        return Arrays.asList(
                new Object[][] {
                    {new CharType(), new CharType(5), false},
                    {new VarCharType(30), new VarCharType(10), false},
                    {new VarCharType(10), new VarCharType(30), true},
                    {new CharType(10), new VarCharType(30), true},
                    {new BinaryType(10), new VarBinaryType(30), true},
                    {new CharType(false, 10), new VarCharType(30), true},
                    {new BinaryType(false, 10), new VarBinaryType(30), true},
                    {new VarCharType(30), new CharType(10), false},
                    {new VarBinaryType(30), new BinaryType(10), false},
                    {new BooleanType(), new BooleanType(false), false},
                    {new BinaryType(10), new BinaryType(30), false},
                    {new VarBinaryType(10), new VarBinaryType(30), true},
                    {new VarBinaryType(30), new VarBinaryType(10), false},
                    {new DecimalType(), new DecimalType(10, 2), false},
                    {new TinyIntType(), new TinyIntType(false), false},
                    {new SmallIntType(), new SmallIntType(false), false},
                    {new IntType(), new IntType(false), false},
                    {new IntType(false), new IntType(), true},
                    {new BigIntType(), new BigIntType(false), false},
                    {new FloatType(), new FloatType(false), false},
                    {new DoubleType(), new DoubleType(false), false},
                    {new DateType(), new DateType(false), false},
                    {new TimeType(), new TimeType(9), false},
                    {new TimestampType(9), new TimestampType(3), false},
                    {new ZonedTimestampType(9), new ZonedTimestampType(3), false},
                    {
                        new ZonedTimestampType(false, TimestampKind.ROWTIME, 9),
                        new ZonedTimestampType(3),
                        false
                    },
                    {
                        new YearMonthIntervalType(
                                YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH, 2),
                        new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.MONTH),
                        false
                    },
                    {
                        new DayTimeIntervalType(
                                DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND, 2, 6),
                        new DayTimeIntervalType(
                                DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND, 2, 7),
                        false
                    },
                    {
                        new ArrayType(new TimestampType()),
                        new ArrayType(new SmallIntType()),
                        false,
                    },
                    {
                        new MultisetType(new TimestampType()),
                        new MultisetType(new SmallIntType()),
                        false
                    },
                    {
                        new MapType(new VarCharType(10), new TimestampType()),
                        new MapType(new VarCharType(30), new TimestampType()),
                        true
                    },
                    {
                        new MapType(new VarCharType(30), new TimestampType()),
                        new MapType(new VarCharType(10), new TimestampType()),
                        false
                    },
                    {
                        new RowType(
                                Arrays.asList(
                                        new RowType.RowField("a", new VarCharType()),
                                        new RowType.RowField("b", new VarCharType()),
                                        new RowType.RowField("c", new VarCharType()),
                                        new RowType.RowField("d", new TimestampType()))),
                        new RowType(
                                Arrays.asList(
                                        new RowType.RowField("_a", new VarCharType()),
                                        new RowType.RowField("_b", new VarCharType()),
                                        new RowType.RowField("_c", new VarCharType()),
                                        new RowType.RowField("_d", new TimestampType()))),
                        // field name doesn't matter
                        true
                    },
                    {
                        new RowType(
                                Arrays.asList(
                                        new RowField("f1", new IntType()),
                                        new RowField("f2", new VarCharType()))),
                        new RowType(
                                Arrays.asList(
                                        new RowField("f1", new IntType()),
                                        new RowField("f2", new BooleanType()))),
                        false
                    },
                    {
                        new ArrayType(
                                new RowType(
                                        Arrays.asList(
                                                new RowField("f1", new IntType()),
                                                new RowField("f2", new IntType())))),
                        new ArrayType(
                                new RowType(
                                        Arrays.asList(
                                                new RowField("f3", new IntType()),
                                                new RowField("f4", new IntType())))),
                        true
                    },
                    {
                        new MapType(
                                new IntType(),
                                new RowType(
                                        Arrays.asList(
                                                new RowField("f1", new IntType()),
                                                new RowField("f2", new IntType())))),
                        new MapType(
                                new IntType(),
                                new RowType(
                                        Arrays.asList(
                                                new RowField("f3", new IntType()),
                                                new RowField("f4", new IntType())))),
                        true
                    },
                    {
                        new MultisetType(
                                new RowType(
                                        Arrays.asList(
                                                new RowField("f1", new IntType()),
                                                new RowField("f2", new IntType())))),
                        new MultisetType(
                                new RowType(
                                        Arrays.asList(
                                                new RowField("f1", new IntType()),
                                                new RowField("f2", new IntType())))),
                        true
                    },
                    {
                        new TypeInformationRawType<>(Types.GENERIC(LogicalTypesTest.class)),
                        new TypeInformationRawType<>(Types.GENERIC(Object.class)),
                        false
                    },
                    {
                        createUserType("User", new IntType(), new VarCharType()),
                        createUserType("User", new IntType(), new VarCharType()),
                        true
                    },
                    {
                        createUserType("User", new IntType(), new VarCharType()),
                        createUserType("User2", new IntType(), new VarCharType()),
                        false
                    },
                    {
                        createDistinctType("Money", new DecimalType(10, 2)),
                        createDistinctType("Money", new DecimalType(10, 2)),
                        true
                    },
                    {
                        createDistinctType("Money", new DecimalType(10, 2)),
                        createDistinctType("Money2", new DecimalType(10, 2)),
                        true
                    },

                    // row and structured type
                    {
                        RowType.of(new IntType(), new VarCharType()),
                        createUserType("User2", new IntType(), new VarCharType()),
                        true
                    },
                    {
                        RowType.of(new BigIntType(), new VarCharType()),
                        createUserType("User2", new IntType(), new VarCharType()),
                        false
                    },
                    {
                        createUserType("User2", new IntType(), new VarCharType()),
                        RowType.of(new IntType(), new VarCharType()),
                        true
                    },
                    {
                        createUserType("User2", new IntType(), new VarCharType()),
                        RowType.of(new BigIntType(), new VarCharType()),
                        false
                    },

                    // test slightly different children of anonymous structured types
                    {
                        StructuredType.newBuilder(Void.class)
                                .attributes(
                                        Arrays.asList(
                                                new StructuredType.StructuredAttribute(
                                                        "f1", new TimestampType()),
                                                new StructuredType.StructuredAttribute(
                                                        "diff", new TinyIntType(false))))
                                .build(),
                        StructuredType.newBuilder(Void.class)
                                .attributes(
                                        Arrays.asList(
                                                new StructuredType.StructuredAttribute(
                                                        "f1", new TimestampType()),
                                                new StructuredType.StructuredAttribute(
                                                        "diff", new TinyIntType(true))))
                                .build(),
                        true
                    },
                    {
                        StructuredType.newBuilder(Void.class)
                                .attributes(
                                        Arrays.asList(
                                                new StructuredType.StructuredAttribute(
                                                        "f1", new TimestampType()),
                                                new StructuredType.StructuredAttribute(
                                                        "diff", new TinyIntType(true))))
                                .build(),
                        StructuredType.newBuilder(Void.class)
                                .attributes(
                                        Arrays.asList(
                                                new StructuredType.StructuredAttribute(
                                                        "f1", new TimestampType()),
                                                new StructuredType.StructuredAttribute(
                                                        "diff", new TinyIntType(false))))
                                .build(),
                        false
                    }
                });
    }

    @Parameter public LogicalType sourceType;

    @Parameter(1)
    public LogicalType targetType;

    @Parameter(2)
    public boolean equals;

    @Test
    public void testSupportsAvoidingCast() {
        assertThat(supportsAvoidingCast(sourceType, targetType), equalTo(equals));
        assertTrue(supportsAvoidingCast(sourceType, sourceType.copy()));
        assertTrue(supportsAvoidingCast(targetType, targetType.copy()));
    }

    private static DistinctType createDistinctType(String name, LogicalType sourceType) {
        return DistinctType.newBuilder(ObjectIdentifier.of("cat", "db", name), sourceType)
                .description("Money type desc.")
                .build();
    }

    private static StructuredType createUserType(String name, LogicalType... children) {
        return StructuredType.newBuilder(ObjectIdentifier.of("cat", "db", name), User.class)
                .attributes(
                        Arrays.stream(children)
                                .map(lt -> new StructuredType.StructuredAttribute("field", lt))
                                .collect(Collectors.toList()))
                .description("User type desc.")
                .setFinal(true)
                .setInstantiable(true)
                .build();
    }

    private static final class User {
        public int setting;
    }
}
