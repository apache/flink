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

package org.apache.flink.fs.s3hadoop;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.fs.s3a.Constants.CHECKSUM_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.CHECKSUM_GENERATION;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HadoopS3AccessHelper}. */
class HadoopS3AccessHelperTest {

    @Test
    void partChecksumsAreDisabledByDefault() {
        assertThat(HadoopS3AccessHelper.partChecksumsEnabled(new Configuration(false))).isFalse();
    }

    @Test
    void partChecksumsAreDisabledForAlgorithmNone() {
        final Configuration conf = new Configuration(false);
        conf.set(CHECKSUM_ALGORITHM, " none ");

        assertThat(HadoopS3AccessHelper.partChecksumsEnabled(conf)).isFalse();
    }

    @Test
    void partChecksumsAreDisabledForEmptyAlgorithm() {
        final Configuration conf = new Configuration(false);
        conf.set(CHECKSUM_ALGORITHM, "");

        assertThat(HadoopS3AccessHelper.partChecksumsEnabled(conf)).isFalse();
    }

    @Test
    void partChecksumsAreEnabledByAlgorithm() {
        final Configuration conf = new Configuration(false);
        conf.set(CHECKSUM_ALGORITHM, "CRC32C");

        assertThat(HadoopS3AccessHelper.partChecksumsEnabled(conf)).isTrue();
    }

    @Test
    void partChecksumsAreEnabledByGeneration() {
        final Configuration conf = new Configuration(false);
        conf.setBoolean(CHECKSUM_GENERATION, true);

        assertThat(HadoopS3AccessHelper.partChecksumsEnabled(conf)).isTrue();
    }
}
