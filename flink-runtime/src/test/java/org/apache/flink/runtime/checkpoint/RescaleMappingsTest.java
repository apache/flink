/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.junit.Test;

import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.mappings;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.set;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.to;
import static org.junit.Assert.assertEquals;

/** Tests {@link RescaleMappings}. */
public class RescaleMappingsTest {
    /**
     * Tests inversion of {@link RescaleMappings}.
     *
     * <pre>
     *     0 -> 0
     *     1 -> 0
     *     2
     *     3 -> 2, 3
     *     4 -> 0, 5
     * </pre>
     */
    @Test
    public void testInvert() {
        RescaleMappings mapping = mappings(to(0), to(0), to(), to(2, 3), to(0, 5));
        RescaleMappings inverted = mapping.invert();
        RescaleMappings expected = mappings(to(0, 1, 4), to(), to(3), to(3), to(), to(4));

        assertEquals(expected, inverted);

        assertEquals(mapping, inverted.invert());
    }

    @Test
    public void testNormalization() {
        RescaleMappings mapping = mappings(to(0), to(0), to(), to(2, 3), to(0, 5), to(), to());

        assertEquals(7, mapping.getNumberOfSources());
        assertEquals(6, mapping.getNumberOfTargets());
        assertEquals(5, mapping.getMappings().length);
    }

    @Test
    public void testAmbiguousTargets() {
        RescaleMappings mapping = mappings(to(0), to(1, 2), to(), to(2, 3, 4), to(4, 5), to());

        assertEquals(set(2, 4), mapping.getAmbiguousTargets());
    }
}
