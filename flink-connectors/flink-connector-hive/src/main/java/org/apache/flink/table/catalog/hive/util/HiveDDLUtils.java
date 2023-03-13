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

package org.apache.flink.table.catalog.hive.util;

/** Util methods for Hive DDL Sql nodes. */
public class HiveDDLUtils {
    // assume ';' cannot be used in column identifiers or type names, otherwise we need to implement
    // escaping
    public static final String COL_DELIMITER = ";";

    private static final byte HIVE_CONSTRAINT_ENABLE = 1 << 2;
    private static final byte HIVE_CONSTRAINT_VALIDATE = 1 << 1;
    private static final byte HIVE_CONSTRAINT_RELY = 1;

    private HiveDDLUtils() {}

    // a constraint is by default ENABLE NOVALIDATE RELY
    public static byte defaultTrait() {
        byte res = enableConstraint((byte) 0);
        res = relyConstraint(res);
        return res;
    }

    // returns a constraint trait that requires ENABLE
    public static byte enableConstraint(byte trait) {
        return (byte) (trait | HIVE_CONSTRAINT_ENABLE);
    }

    // returns a constraint trait that doesn't require ENABLE
    public static byte disableConstraint(byte trait) {
        return (byte) (trait & (~HIVE_CONSTRAINT_ENABLE));
    }

    // returns a constraint trait that requires VALIDATE
    public static byte validateConstraint(byte trait) {
        return (byte) (trait | HIVE_CONSTRAINT_VALIDATE);
    }

    // returns a constraint trait that doesn't require VALIDATE
    public static byte noValidateConstraint(byte trait) {
        return (byte) (trait & (~HIVE_CONSTRAINT_VALIDATE));
    }

    // returns a constraint trait that requires RELY
    public static byte relyConstraint(byte trait) {
        return (byte) (trait | HIVE_CONSTRAINT_RELY);
    }

    // returns whether a trait requires ENABLE constraint
    public static boolean requireEnableConstraint(byte trait) {
        return (trait & HIVE_CONSTRAINT_ENABLE) != 0;
    }

    // returns whether a trait requires VALIDATE constraint
    public static boolean requireValidateConstraint(byte trait) {
        return (trait & HIVE_CONSTRAINT_VALIDATE) != 0;
    }

    // returns whether a trait requires RELY constraint
    public static boolean requireRelyConstraint(byte trait) {
        return (trait & HIVE_CONSTRAINT_RELY) != 0;
    }
}
