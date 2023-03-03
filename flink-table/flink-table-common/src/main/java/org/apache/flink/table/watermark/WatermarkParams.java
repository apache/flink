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

package org.apache.flink.table.watermark;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.factories.FactoryUtil;

import java.io.Serializable;
import java.util.Objects;

/** Pojo class for watermark configs from table options or 'OPTIONS' hint. */
@Internal
public class WatermarkParams implements Serializable {
    private static final long serialVersionUID = 1L;

    private WatermarkEmitStrategy emitStrategy;

    public WatermarkParams() {}

    public WatermarkParams(WatermarkEmitStrategy emitStrategy) {
        this.emitStrategy = emitStrategy;
    }

    public WatermarkEmitStrategy getEmitStrategy() {
        return emitStrategy;
    }

    public void setEmitStrategy(WatermarkEmitStrategy emitStrategy) {
        this.emitStrategy = emitStrategy;
    }

    public static WatermarkParamsBuilder builder() {
        return new WatermarkParamsBuilder();
    }

    @Override
    public String toString() {
        return "WatermarkParams{" + "emitStrategy=" + emitStrategy + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WatermarkParams that = (WatermarkParams) o;
        return emitStrategy == that.emitStrategy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(emitStrategy);
    }

    /** Builder of WatermarkHintParams. */
    public static class WatermarkParamsBuilder {
        private WatermarkEmitStrategy emitStrategy =
                FactoryUtil.WATERMARK_EMIT_STRATEGY.defaultValue();

        public WatermarkParamsBuilder emitStrategy(WatermarkEmitStrategy emitStrategy) {
            this.emitStrategy = emitStrategy;
            return this;
        }

        public WatermarkParams build() {
            return new WatermarkParams(emitStrategy);
        }
    }
}
