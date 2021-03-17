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

package org.apache.flink.api.common.resources;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for {@link CPUResource}. */
public class CPUResourceTest extends TestLogger {
    @Test
    public void toHumanReadableString() {
        assertThat(new CPUResource(0).toHumanReadableString(), is("0.00 cores"));
        assertThat(new CPUResource(1).toHumanReadableString(), is("1.00 cores"));
        assertThat(new CPUResource(1.2).toHumanReadableString(), is("1.20 cores"));
        assertThat(new CPUResource(1.23).toHumanReadableString(), is("1.23 cores"));
        assertThat(new CPUResource(1.234).toHumanReadableString(), is("1.23 cores"));
        assertThat(new CPUResource(1.235).toHumanReadableString(), is("1.24 cores"));
        assertThat(new CPUResource(10).toHumanReadableString(), is("10.00 cores"));
        assertThat(new CPUResource(100).toHumanReadableString(), is("100.00 cores"));
        assertThat(new CPUResource(1000).toHumanReadableString(), is("1000.00 cores"));
        assertThat(new CPUResource(123456789).toHumanReadableString(), is("123456789.00 cores"));
        assertThat(new CPUResource(12345.6789).toHumanReadableString(), is("12345.68 cores"));
    }
}
