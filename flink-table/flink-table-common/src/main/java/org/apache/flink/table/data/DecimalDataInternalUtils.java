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

package org.apache.flink.table.data;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.DecimalType;

import java.math.BigDecimal;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Internal factories for {@link DecimalData}. */
@Internal
public final class DecimalDataInternalUtils {

    private DecimalDataInternalUtils() {}

    /**
     * Like {@link DecimalData#fromUnscaledLong}, but for a non-compact precision, keeping the value
     * as a long instead of a {@link BigDecimal}. Any long fits a non-compact precision.
     */
    public static DecimalData fromUnscaledLong(long unscaledLong, int precision, int scale) {
        checkArgument(!DecimalData.isCompact(precision) && precision <= DecimalType.MAX_PRECISION);
        return new DecimalData(precision, scale, unscaledLong, null);
    }
}
