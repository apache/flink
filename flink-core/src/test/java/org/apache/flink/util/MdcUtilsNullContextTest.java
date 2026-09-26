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

package org.apache.flink.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.slf4j.MDC;
import org.slf4j.helpers.BasicMDCAdapter;
import org.slf4j.spi.MDCAdapter;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Isolated
class MdcUtilsNullContextTest {

    private MDCAdapter originalAdapter;

    @BeforeEach
    void setUp() throws Exception {
        originalAdapter = getCurrentMDCAdapter();
        setMDCAdapter(new NullRejectingMDCAdapter());
    }

    @AfterEach
    void tearDown() throws Exception {
        setMDCAdapter(originalAdapter);
    }

    /**
     * Restoring an absent (null) MDC context must never fail.
     *
     * <p>Backends are free to return null from {@link MDC#getCopyOfContextMap()}, and SLF4J API did
     * not require them to accept that null back in {@link MDC#setContextMap(Map)} until SLF4J
     * 2.0.0.
     *
     * <ul>
     *   <li>Logback 1.2 (SLF4J 1.7) - can return null, rejects null
     *   <li>Logback 1.3.0-1.3.1 (SLF4J 2) - can return null, rejects null
     *   <li>Log4j 2.26.1 (both SLF4J versions) - never returns null, rejects null
     * </ul>
     *
     * <p>See https://issues.apache.org/jira/browse/FLINK-36227 for historical context.
     */
    @Test
    void testContextRestorationWorksWithNullContext() {
        assertThat(MDC.getCopyOfContextMap()).isNull();
        assertThrows(NullPointerException.class, () -> MDC.setContextMap(null));

        MdcUtils.MdcCloseable restoreContext =
                MdcUtils.withContext(Collections.singletonMap("k", "v"));
        assertThat(MDC.get("k")).isEqualTo("v");
        assertDoesNotThrow(restoreContext::close);
        assertThat(MDC.get("k")).isNull();
    }

    private MDCAdapter getCurrentMDCAdapter() throws Exception {
        Field adapterField = MDC.class.getDeclaredField("MDC_ADAPTER");
        adapterField.setAccessible(true);
        return (MDCAdapter) adapterField.get(null);
    }

    private void setMDCAdapter(MDCAdapter adapter) throws Exception {
        Field adapterField = MDC.class.getDeclaredField("MDC_ADAPTER");
        adapterField.setAccessible(true);
        adapterField.set(null, adapter);
    }

    private static class NullRejectingMDCAdapter extends BasicMDCAdapter {
        @Override
        public Map<String, String> getCopyOfContextMap() {
            return null;
        }

        @Override
        public void setContextMap(Map<String, String> contextMap) {
            if (contextMap == null) {
                throw new NullPointerException("contextMap cannot be null");
            }
            super.setContextMap(contextMap);
        }
    }
}
