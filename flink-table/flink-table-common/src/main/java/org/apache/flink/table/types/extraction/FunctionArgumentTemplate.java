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

package org.apache.flink.table.types.extraction;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.InputTypeStrategies;

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.flink.table.types.extraction.ExtractionUtils.extractionError;

/**
 * Template of a function argument. It can either be backed by a single or a group of {@link
 * DataType}s.
 */
@Internal
final class FunctionArgumentTemplate {

    final @Nullable DataType dataType;

    final @Nullable InputGroup inputGroup;

    private FunctionArgumentTemplate(@Nullable DataType dataType, @Nullable InputGroup inputGroup) {
        this.dataType = dataType;
        this.inputGroup = inputGroup;
    }

    static FunctionArgumentTemplate of(DataType dataType) {
        return new FunctionArgumentTemplate(dataType, null);
    }

    static FunctionArgumentTemplate of(InputGroup inputGroup) {
        return new FunctionArgumentTemplate(null, inputGroup);
    }

    ArgumentTypeStrategy toArgumentTypeStrategy() {
        if (dataType != null) {
            return InputTypeStrategies.explicit(dataType);
        }
        assert inputGroup != null;
        switch (inputGroup) {
            case ANY:
                return InputTypeStrategies.ANY;
            case UNKNOWN:
            default:
                throw extractionError("Unsupported input group.");
        }
    }

    Class<?> toConversionClass() {
        if (dataType != null) {
            return dataType.getConversionClass();
        }
        assert inputGroup != null;
        switch (inputGroup) {
            case ANY:
                return Object.class;
            case UNKNOWN:
            default:
                throw extractionError("Unsupported input group.");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FunctionArgumentTemplate that = (FunctionArgumentTemplate) o;
        return Objects.equals(dataType, that.dataType) && inputGroup == that.inputGroup;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataType, inputGroup);
    }
}
