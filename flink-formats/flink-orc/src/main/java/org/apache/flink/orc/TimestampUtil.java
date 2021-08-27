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

package org.apache.flink.orc;

import org.apache.flink.orc.vector.OrcLegacyTimestampColumnVector;
import org.apache.flink.orc.vector.OrcTimestampColumnVector;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class to handle timestamp vectors. It's responsible for deciding whether new or legacy (Hive
 * 2.0.x) timestamp column vector should be used.
 */
public class TimestampUtil {

    private TimestampUtil() {}

    private static final Logger LOG = LoggerFactory.getLogger(OrcLegacyTimestampColumnVector.class);

    private static Class hiveTSColVectorClz = null;

    static {
        try {
            hiveTSColVectorClz =
                    Class.forName("org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector");
        } catch (ClassNotFoundException e) {
            LOG.debug("Hive TimestampColumnVector not available", e);
        }
    }

    // whether a ColumnVector is the new TimestampColumnVector
    public static boolean isHiveTimestampColumnVector(ColumnVector vector) {
        return hiveTSColVectorClz != null && hiveTSColVectorClz.isAssignableFrom(vector.getClass());
    }

    // creates a Hive ColumnVector of constant timestamp value
    public static ColumnVector createVectorFromConstant(int batchSize, Object value) {
        if (hiveTSColVectorClz != null) {
            return OrcTimestampColumnVector.createFromConstant(batchSize, value);
        } else {
            return OrcLegacyTimestampColumnVector.createFromConstant(batchSize, value);
        }
    }
}
