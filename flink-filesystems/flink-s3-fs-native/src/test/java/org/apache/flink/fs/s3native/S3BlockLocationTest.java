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

package org.apache.flink.fs.s3native;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link S3BlockLocation}. */
class S3BlockLocationTest {

    @Test
    void testAccessors() {
        S3BlockLocation loc = new S3BlockLocation(new String[] {"localhost"}, 100L, 512L);

        assertThat(loc.getHosts()).containsExactly("localhost");
        assertThat(loc.getOffset()).isEqualTo(100L);
        assertThat(loc.getLength()).isEqualTo(512L);
    }

    @Test
    void testCompareToOrdersByOffset() {
        S3BlockLocation first = new S3BlockLocation(new String[] {"localhost"}, 0L, 100L);
        S3BlockLocation second = new S3BlockLocation(new String[] {"localhost"}, 100L, 100L);

        assertThat(first.compareTo(second)).isNegative();
        assertThat(second.compareTo(first)).isPositive();
        assertThat(first.compareTo(first)).isZero();
    }
}
