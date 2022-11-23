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

package org.apache.flink.table.module.hive.udf.generic;

import org.apache.flink.shaded.guava30.com.google.common.base.CharMatcher;

import org.apache.hadoop.hive.ql.udf.UDFToBoolean;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Counterpart of Hive's org.apache.hadoop.hive.ql.udf.UDFToBoolean, but adjust the logic for
 * convert string to boolean.
 */
public class HiveUDFToBoolean extends UDFToBoolean {

    private final BooleanWritable booleanWritable = new BooleanWritable();

    private static final Set<String> TRUE_STRINGS = new HashSet<>();
    private static final Set<String> FALSE_STRINGS = new HashSet<>();

    static {
        TRUE_STRINGS.addAll(Arrays.asList("t", "true", "y", "yes", "1"));
        FALSE_STRINGS.addAll(Arrays.asList("f", "false", "n", "no", "0"));
    }

    /**
     * Convert from a string to boolean. This is called for CAST(... AS BOOLEAN)
     *
     * @param i The string value to convert
     * @return BooleanWritable
     */
    public BooleanWritable evaluate(Text i) {
        if (i == null) {
            return null;
        }

        String s = CharMatcher.whitespace().trimFrom(i.toString()).toLowerCase();
        if (TRUE_STRINGS.contains(s)) {
            booleanWritable.set(true);
            return booleanWritable;
        } else if (FALSE_STRINGS.contains(s)) {
            booleanWritable.set(false);
            return booleanWritable;
        } else {
            return null;
        }
    }
}
