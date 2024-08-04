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

package org.apache.flink.watermark;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.TimestampWatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.Serializable;

/**
 * This class defines watermark handling policy for ProcessOperator. Note that implementations of
 * this interface must ensure to provide the default constructor.
 */
@Internal
public interface IdentifiableWatermark extends Serializable {
    String getWatermarkIdentifier();

    String TIMESTAMP_WATERMARK_IDENTIFIER = "TimestampWatermark";
    String INTERNAL_TIMESTAMP_WATERMARK_IDENTIFIER = "InternalTimestampWatermark";

    static String generateIdentifier(WatermarkDeclaration declaration) {
        if (declaration instanceof LongWatermarkDeclaration) {
            LongWatermarkDeclaration longWatermarkDeclaration =
                    (LongWatermarkDeclaration) declaration;
            return "LongWatermark(" + longWatermarkDeclaration.getComparisonSemantics() + ")";
        } else if (declaration instanceof TimestampWatermarkDeclaration) {
            return TIMESTAMP_WATERMARK_IDENTIFIER;
        } else {
            throw new FlinkRuntimeException("Unknown watermark declaration: " + declaration);
        }
    }

    static String generateIdentifier(InternalWatermarkDeclaration declaration) {
        if (declaration instanceof InternalLongWatermarkDeclaration) {
            LongWatermarkDeclaration longWatermarkDeclaration =
                    (LongWatermarkDeclaration) declaration;
            return "LongWatermark(" + longWatermarkDeclaration.getComparisonSemantics() + ")";
        } else if (declaration instanceof InternalTimestampWatermarkDeclaration) {
            return TIMESTAMP_WATERMARK_IDENTIFIER;
        } else if (declaration instanceof InternalTimestampInternalWatermarkDeclaration) {
            return INTERNAL_TIMESTAMP_WATERMARK_IDENTIFIER;
        } else {
            throw new FlinkRuntimeException("Unknown watermark declaration: " + declaration);
        }
    }
}
