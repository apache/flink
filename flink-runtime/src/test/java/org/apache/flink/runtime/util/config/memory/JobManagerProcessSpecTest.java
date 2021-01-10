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

package org.apache.flink.runtime.util.config.memory;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.jobmanager.JobManagerProcessSpec;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

/** Tests for {@link JobManagerProcessSpec}. */
public class JobManagerProcessSpecTest extends TestLogger {
    @Test
    public void testEquals() {
        JobManagerProcessSpec spec1 =
                new JobManagerProcessSpec(
                        MemorySize.parse("1m"),
                        MemorySize.parse("2m"),
                        MemorySize.parse("3m"),
                        MemorySize.parse("4m"));

        JobManagerProcessSpec spec2 =
                new JobManagerProcessSpec(
                        MemorySize.parse("1m"),
                        MemorySize.parse("2m"),
                        MemorySize.parse("3m"),
                        MemorySize.parse("4m"));

        assertThat(spec1, is(spec2));
    }

    @Test
    public void testNotEquals() {
        JobManagerProcessSpec spec1 =
                new JobManagerProcessSpec(
                        MemorySize.parse("1m"),
                        MemorySize.parse("2m"),
                        MemorySize.parse("3m"),
                        MemorySize.parse("4m"));

        JobManagerProcessSpec spec2 =
                new JobManagerProcessSpec(
                        MemorySize.ZERO, MemorySize.ZERO, MemorySize.ZERO, MemorySize.ZERO);

        assertThat(spec1, not(spec2));
    }
}
