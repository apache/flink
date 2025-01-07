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

@Isolated
public class MdcLogbackCompatibilityTest {

    private MDCAdapter originalAdapter;

    @BeforeEach
    void setUp() throws Exception {
        originalAdapter = getCurrentMDCAdapter();
        setMDCAdapter(new BasicMDCAdapter());
    }

    @AfterEach
    void tearDown() throws Exception {
        setMDCAdapter(originalAdapter);
    }

    /**
     * The {@link MDC#setContextMap(Map)} method in Logback 1.2 does not accept nulls, unlike Log4j
     * and Logback 1.3.2. See https://issues.apache.org/jira/browse/FLINK-36227 for details.
     */
    @Test
    void testContextRestorationWorksWithNullContext() {
        assertThat(MDC.getCopyOfContextMap()).isNull();

        MdcUtils.MdcCloseable restoreContext =
                MdcUtils.withContext(Collections.singletonMap("k", "v"));
        assertThat(MDC.get("k")).isEqualTo("v");
        assertDoesNotThrow(restoreContext::close);
        assertThat(MDC.get("k")).isNull();
    }

    private MDCAdapter getCurrentMDCAdapter() throws Exception {
        Field adapterField = MDC.class.getDeclaredField("mdcAdapter");
        adapterField.setAccessible(true);
        return (MDCAdapter) adapterField.get(null);
    }

    private void setMDCAdapter(MDCAdapter adapter) throws Exception {
        Field adapterField = MDC.class.getDeclaredField("mdcAdapter");
        adapterField.setAccessible(true);
        adapterField.set(null, adapter);
    }
}
