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

package org.apache.flink.api.common.operators;

import org.apache.flink.api.common.operators.SemanticProperties.InvalidSemanticAnnotationException;
import org.apache.flink.api.common.operators.util.FieldSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SingleInputSemanticPropertiesTest {

    @Test
    void testGetTargetFields() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        sp.addForwardedField(0, 1);
        sp.addForwardedField(1, 4);
        sp.addForwardedField(2, 3);
        sp.addForwardedField(3, 2);

        assertThat(sp.getForwardingTargetFields(0, 0)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(0, 1)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(0, 2)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(0, 3)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4)).isNotNull();
        assertThat(sp.getForwardingTargetFields(0, 4)).isEmpty();

        sp = new SingleInputSemanticProperties();
        sp.addForwardedField(0, 0);
        sp.addForwardedField(0, 4);
        sp.addForwardedField(1, 1);
        sp.addForwardedField(1, 2);
        sp.addForwardedField(1, 3);

        assertThat(sp.getForwardingTargetFields(0, 0)).hasSize(2);
        assertThat(sp.getForwardingTargetFields(0, 1)).hasSize(3);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2)).isNotNull();
        assertThat(sp.getForwardingTargetFields(0, 2)).isEmpty();
    }

    @Test
    void testGetSourceField() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        sp.addForwardedField(0, 1);
        sp.addForwardedField(1, 4);
        sp.addForwardedField(2, 3);
        sp.addForwardedField(3, 2);

        assertThat(sp.getForwardingSourceField(0, 1)).isZero();
        assertThat(sp.getForwardingSourceField(0, 4)).isOne();
        assertThat(sp.getForwardingSourceField(0, 3)).isEqualTo(2);
        assertThat(sp.getForwardingSourceField(0, 2)).isEqualTo(3);
        assertThat(sp.getForwardingSourceField(0, 0)).isNegative();
        assertThat(sp.getForwardingSourceField(0, 5)).isNegative();

        sp = new SingleInputSemanticProperties();
        sp.addForwardedField(0, 0);
        sp.addForwardedField(0, 4);
        sp.addForwardedField(1, 1);
        sp.addForwardedField(1, 2);
        sp.addForwardedField(1, 3);

        assertThat(sp.getForwardingSourceField(0, 0)).isZero();
        assertThat(sp.getForwardingSourceField(0, 4)).isZero();
        assertThat(sp.getForwardingSourceField(0, 1)).isOne();
        assertThat(sp.getForwardingSourceField(0, 2)).isOne();
        assertThat(sp.getForwardingSourceField(0, 3)).isOne();
        assertThat(sp.getForwardingSourceField(0, 5)).isNegative();
    }

    @Test
    void testGetReadSet() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        sp.addReadFields(new FieldSet(0, 1));

        assertThat(sp.getReadFields(0)).hasSize(2);
        assertThat(sp.getReadFields(0).contains(0)).isTrue();
        assertThat(sp.getReadFields(0).contains(1)).isTrue();

        sp.addReadFields(new FieldSet(3));

        assertThat(sp.getReadFields(0)).hasSize(3);
        assertThat(sp.getReadFields(0).contains(0)).isTrue();
        assertThat(sp.getReadFields(0).contains(1)).isTrue();
        assertThat(sp.getReadFields(0).contains(3)).isTrue();
    }

    @Test
    void testAddForwardedFieldsTargetTwice() {
        assertThatThrownBy(
                        () -> {
                            SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
                            sp.addForwardedField(0, 2);
                            sp.addForwardedField(1, 2);
                        })
                .isInstanceOf(InvalidSemanticAnnotationException.class);
    }

    @Test
    void testGetTargetFieldInvalidIndex() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        sp.addForwardedField(0, 0);

        assertThatThrownBy(() -> sp.getForwardingTargetFields(1, 0))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testGetSourceFieldInvalidIndex() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        sp.addForwardedField(0, 0);

        assertThatThrownBy(() -> sp.getForwardingSourceField(1, 0))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testGetReadFieldsInvalidIndex() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        sp.addReadFields(new FieldSet(0, 1));

        assertThatThrownBy(() -> sp.getReadFields(1)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testAllForwardedSingleInputSemProps() {

        SingleInputSemanticProperties sp =
                new SingleInputSemanticProperties.AllFieldsForwardedProperties();

        assertThat(sp.getForwardingTargetFields(0, 0)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(0, 1)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(0, 123)).hasSize(1);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 123).contains(123)).isTrue();

        assertThat(sp.getForwardingSourceField(0, 0)).isZero();
        assertThat(sp.getForwardingSourceField(0, 2)).isEqualTo(2);
        assertThat(sp.getForwardingSourceField(0, 123)).isEqualTo(123);
    }

    @Test
    void testAllForwardedSingleInputSemPropsInvalidIndex1() {

        assertThatThrownBy(
                        () -> {
                            SingleInputSemanticProperties sp =
                                    new SingleInputSemanticProperties
                                            .AllFieldsForwardedProperties();
                            sp.getForwardingSourceField(1, 0);
                        })
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testAllForwardedSingleInputSemPropsInvalidIndex2() {

        assertThatThrownBy(
                        () -> {
                            SingleInputSemanticProperties sp =
                                    new SingleInputSemanticProperties
                                            .AllFieldsForwardedProperties();
                            sp.getForwardingTargetFields(1, 0);
                        })
                .isInstanceOf(IndexOutOfBoundsException.class);
    }
}
