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

package org.apache.flink.datastream.impl.common;

import org.apache.flink.api.common.typeinfo.utils.TypeUtils;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link org.apache.flink.api.common.typeinfo.utils.TypeUtils}. */
class TypeUtilsTest {

    @Test
    void testNewInstanceWithPrimitiveArg() throws ReflectiveOperationException {
        assertThat(
                        TypeUtils.getInstance(
                                        "org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus",
                                        0)
                                .toString())
                .isEqualTo("WatermarkStatus(ACTIVE)");
    }

    @Test
    void testNewInstanceWithObjectArg() throws ReflectiveOperationException {
        assertThat(
                        TypeUtils.getInstance(
                                        "org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer",
                                        new BooleanSerializer())
                                .toString())
                .contains("StreamElementSerializer");
    }

    @Test
    void testNewInstanceWithIncorrectConstructorArgType() {
        assertThatThrownBy(
                        () ->
                                TypeUtils.getInstance(
                                        "org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer",
                                        123))
                .isInstanceOf(NoSuchMethodException.class);
    }

    @Test
    void testNewInstanceWithIncorrectConstructorArgCount() {
        assertThatThrownBy(
                        () ->
                                TypeUtils.getInstance(
                                        "org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer"))
                .isInstanceOf(NoSuchMethodException.class);
    }
}
