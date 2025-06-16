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

package org.apache.flink.table.ml;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TaskType}. */
public class TaskTypeTest {

    @Test
    public void testEnumValues() {
        assertThat(TaskType.values()).hasSize(5);
        assertThat(TaskType.valueOf("REGRESSION")).isEqualTo(TaskType.REGRESSION);
        assertThat(TaskType.valueOf("CLUSTERING")).isEqualTo(TaskType.CLUSTERING);
        assertThat(TaskType.valueOf("CLASSIFICATION")).isEqualTo(TaskType.CLASSIFICATION);
        assertThat(TaskType.valueOf("EMBEDDING")).isEqualTo(TaskType.EMBEDDING);
        assertThat(TaskType.valueOf("TEXT_GENERATION")).isEqualTo(TaskType.TEXT_GENERATION);
    }

    @Test
    public void testGetName() {
        assertThat(TaskType.REGRESSION.getName()).isEqualTo("regression");
        assertThat(TaskType.CLUSTERING.getName()).isEqualTo("clustering");
        assertThat(TaskType.CLASSIFICATION.getName()).isEqualTo("classification");
        assertThat(TaskType.EMBEDDING.getName()).isEqualTo("embedding");
        assertThat(TaskType.TEXT_GENERATION.getName()).isEqualTo("text_generation");
    }

    @Test
    public void testFromName() {
        assertThat(TaskType.fromName("regression")).isEqualTo(TaskType.REGRESSION);
        assertThat(TaskType.fromName("clustering")).isEqualTo(TaskType.CLUSTERING);
        assertThat(TaskType.fromName("classification")).isEqualTo(TaskType.CLASSIFICATION);
        assertThat(TaskType.fromName("embedding")).isEqualTo(TaskType.EMBEDDING);
        assertThat(TaskType.fromName("text_generation")).isEqualTo(TaskType.TEXT_GENERATION);
    }

    @Test
    public void testFromNameWithInvalidName() {
        assertThatThrownBy(() -> TaskType.fromName("invalid_task_type"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testIsValidTaskType() {
        assertThat(TaskType.isValidTaskType("regression")).isTrue();
        assertThat(TaskType.isValidTaskType("clustering")).isTrue();
        assertThat(TaskType.isValidTaskType("classification")).isTrue();
        assertThat(TaskType.isValidTaskType("embedding")).isTrue();
        assertThat(TaskType.isValidTaskType("text_generation")).isTrue();

        assertThat(TaskType.isValidTaskType("invalid_task_type")).isFalse();
        assertThat(TaskType.isValidTaskType("")).isFalse();
        assertThat(TaskType.isValidTaskType(null)).isFalse();
    }
}
