/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for OutputColsHelper. */
public class OutputColsHelperTest {

    private TableSchema tableSchema =
            new TableSchema(
                    new String[] {"f0", "f1", "f2"},
                    new TypeInformation[] {
                        TypeInformation.of(String.class),
                        TypeInformation.of(Long.class),
                        TypeInformation.of(Integer.class)
                    });
    private String[] reservedColNames = new String[] {"f0"};
    private Row row = Row.of("a", 1L, 1);

    @Test
    public void testResultSchema() {
        TableSchema expectSchema =
                new TableSchema(
                        new String[] {"f0", "f1", "f2", "res"},
                        new TypeInformation[] {
                            TypeInformation.of(String.class),
                            TypeInformation.of(Long.class),
                            TypeInformation.of(Integer.class),
                            TypeInformation.of(String.class)
                        });
        OutputColsHelper helper =
                new OutputColsHelper(tableSchema, "res", TypeInformation.of(String.class));
        Assertions.assertEquals(expectSchema, helper.getResultSchema());

        expectSchema =
                new TableSchema(
                        new String[] {"f0", "res"},
                        new TypeInformation[] {
                            TypeInformation.of(String.class), TypeInformation.of(String.class)
                        });
        helper =
                new OutputColsHelper(
                        tableSchema, "res", TypeInformation.of(String.class), reservedColNames);
        Assertions.assertEquals(expectSchema, helper.getResultSchema());
        Assertions.assertArrayEquals(reservedColNames, helper.getReservedColumns());

        expectSchema =
                new TableSchema(
                        new String[] {"f0", "res1", "res2"},
                        new TypeInformation[] {
                            TypeInformation.of(String.class),
                            TypeInformation.of(String.class),
                            TypeInformation.of(Integer.class)
                        });
        helper =
                new OutputColsHelper(
                        tableSchema,
                        new String[] {"res1", "res2"},
                        new TypeInformation[] {
                            TypeInformation.of(String.class), TypeInformation.of(Integer.class)
                        },
                        reservedColNames);
        Assertions.assertEquals(expectSchema, helper.getResultSchema());

        expectSchema =
                new TableSchema(
                        new String[] {"f0", "f1", "f2", "res"},
                        new TypeInformation[] {
                            TypeInformation.of(String.class),
                            TypeInformation.of(Long.class),
                            TypeInformation.of(Integer.class),
                            TypeInformation.of(String.class)
                        });
        helper = new OutputColsHelper(tableSchema, "res", TypeInformation.of(String.class));
        Assertions.assertEquals(expectSchema, helper.getResultSchema());
        Assertions.assertArrayEquals(tableSchema.getFieldNames(), helper.getReservedColumns());

        expectSchema =
                new TableSchema(
                        new String[] {"f0", "f1", "f2"},
                        new TypeInformation[] {
                            TypeInformation.of(Integer.class),
                            TypeInformation.of(Long.class),
                            TypeInformation.of(Integer.class)
                        });
        helper = new OutputColsHelper(tableSchema, "f0", TypeInformation.of(Integer.class));
        Assertions.assertEquals(expectSchema, helper.getResultSchema());

        expectSchema =
                new TableSchema(
                        new String[] {"f0", "f1", "f2"},
                        new TypeInformation[] {
                            TypeInformation.of(Integer.class),
                            TypeInformation.of(Long.class),
                            TypeInformation.of(String.class)
                        });
        helper =
                new OutputColsHelper(
                        tableSchema,
                        new String[] {"f0", "f2"},
                        new TypeInformation[] {
                            TypeInformation.of(Integer.class), TypeInformation.of(String.class)
                        });
        Assertions.assertEquals(expectSchema, helper.getResultSchema());

        expectSchema =
                new TableSchema(
                        new String[] {"f0", "res"},
                        new TypeInformation[] {
                            TypeInformation.of(String.class), TypeInformation.of(Integer.class)
                        });
        helper =
                new OutputColsHelper(
                        tableSchema,
                        new String[] {"res", "f0"},
                        new TypeInformation[] {
                            TypeInformation.of(Integer.class), TypeInformation.of(String.class)
                        },
                        reservedColNames);
        Assertions.assertEquals(expectSchema, helper.getResultSchema());

        expectSchema =
                new TableSchema(
                        new String[] {"f0", "f1", "res"},
                        new TypeInformation[] {
                            TypeInformation.of(String.class),
                            TypeInformation.of(Long.class),
                            TypeInformation.of(Integer.class)
                        });
        helper =
                new OutputColsHelper(
                        tableSchema,
                        new String[] {"res"},
                        new TypeInformation[] {
                            TypeInformation.of(Integer.class), TypeInformation.of(String.class)
                        },
                        new String[] {"f1", "f0"});
        Assertions.assertEquals(expectSchema, helper.getResultSchema());
    }

    @Test
    public void testResultRow() {
        OutputColsHelper helper =
                new OutputColsHelper(tableSchema, "res", TypeInformation.of(String.class));
        Row expectRow = Row.of("a", 1L, 1, "b");
        Assertions.assertEquals(helper.getResultRow(row, Row.of("b")), expectRow);

        helper =
                new OutputColsHelper(
                        tableSchema,
                        new String[] {"res1", "res2"},
                        new TypeInformation[] {
                            TypeInformation.of(String.class), TypeInformation.of(Integer.class)
                        });
        expectRow = Row.of("a", 1L, 1, "b", 2);
        Assertions.assertEquals(expectRow, helper.getResultRow(row, Row.of("b", 2)));

        helper =
                new OutputColsHelper(
                        tableSchema,
                        new String[] {"res", "f0"},
                        new TypeInformation[] {
                            TypeInformation.of(Integer.class), TypeInformation.of(String.class)
                        },
                        reservedColNames);
        expectRow = Row.of("b", 2);
        Assertions.assertEquals(expectRow, helper.getResultRow(row, Row.of(2, "b")));
    }

    @Test
    public void testExceptionCase() {
        TableSchema expectSchema =
                new TableSchema(
                        new String[] {"f0", "res"},
                        new TypeInformation[] {
                            TypeInformation.of(String.class), TypeInformation.of(Integer.class)
                        });
        OutputColsHelper helper =
                new OutputColsHelper(
                        tableSchema,
                        new String[] {"res", "f0"},
                        new TypeInformation[] {
                            TypeInformation.of(Integer.class), TypeInformation.of(String.class)
                        },
                        new String[] {"res", "res2"});
        Assertions.assertEquals(expectSchema, helper.getResultSchema());

        expectSchema =
                new TableSchema(
                        new String[] {"f0", "f1", "res"},
                        new TypeInformation[] {
                            TypeInformation.of(String.class),
                            TypeInformation.of(Long.class),
                            TypeInformation.of(Integer.class)
                        });
        helper =
                new OutputColsHelper(
                        tableSchema,
                        new String[] {"res", "f0"},
                        new TypeInformation[] {
                            TypeInformation.of(Integer.class), TypeInformation.of(String.class)
                        },
                        new String[] {"f1", "res"});
        Assertions.assertEquals(expectSchema, helper.getResultSchema());

        expectSchema =
                new TableSchema(
                        new String[] {"f0", "f1", "f2"},
                        new TypeInformation[] {
                            TypeInformation.of(String.class),
                            TypeInformation.of(Integer.class),
                            TypeInformation.of(Double.class)
                        });
        helper =
                new OutputColsHelper(
                        tableSchema,
                        new String[] {"f1", "f0", "f2"},
                        new TypeInformation[] {
                            TypeInformation.of(Integer.class),
                            TypeInformation.of(String.class),
                            TypeInformation.of(Double.class)
                        },
                        new String[] {"f1", "res"});
        Assertions.assertEquals(expectSchema, helper.getResultSchema());
    }
}
