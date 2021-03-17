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

package org.apache.flink.table.types.extraction.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ExtractionVersion;
import org.apache.flink.table.annotation.HintFlag;
import org.apache.flink.table.annotation.InputGroup;

import java.lang.annotation.Annotation;

/** Mock for {@link DataTypeHint}. Methods can be overridden if necessary. */
@SuppressWarnings("ClassExplicitlyAnnotation")
public class DataTypeHintMock implements DataTypeHint {

    private static final DataTypeHint DEFAULT_ANNOTATION = getDefaultAnnotation();

    @Override
    public String value() {
        return DEFAULT_ANNOTATION.value();
    }

    @Override
    public Class<?> bridgedTo() {
        return DEFAULT_ANNOTATION.bridgedTo();
    }

    @Override
    public Class<? extends TypeSerializer<?>> rawSerializer() {
        return DEFAULT_ANNOTATION.rawSerializer();
    }

    @Override
    public InputGroup inputGroup() {
        return DEFAULT_ANNOTATION.inputGroup();
    }

    @Override
    public ExtractionVersion version() {
        return DEFAULT_ANNOTATION.version();
    }

    @Override
    public HintFlag allowRawGlobally() {
        return DEFAULT_ANNOTATION.allowRawGlobally();
    }

    @Override
    public String[] allowRawPattern() {
        return DEFAULT_ANNOTATION.allowRawPattern();
    }

    @Override
    public String[] forceRawPattern() {
        return DEFAULT_ANNOTATION.forceRawPattern();
    }

    @Override
    public int defaultDecimalPrecision() {
        return DEFAULT_ANNOTATION.defaultDecimalPrecision();
    }

    @Override
    public int defaultDecimalScale() {
        return DEFAULT_ANNOTATION.defaultDecimalScale();
    }

    @Override
    public int defaultYearPrecision() {
        return DEFAULT_ANNOTATION.defaultYearPrecision();
    }

    @Override
    public int defaultSecondPrecision() {
        return DEFAULT_ANNOTATION.defaultSecondPrecision();
    }

    @Override
    public Class<? extends Annotation> annotationType() {
        return DataTypeHint.class;
    }

    // --------------------------------------------------------------------------------------------

    @DataTypeHint
    private static class DefaultAnnotationHelper {
        // no implementation
    }

    private static DataTypeHint getDefaultAnnotation() {
        return DefaultAnnotationHelper.class.getAnnotation(DataTypeHint.class);
    }
}
