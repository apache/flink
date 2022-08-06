package org.apache.flink.table.runtime.functions.table;

import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.runtime.functions.table.lookup.CachingLookupFunction;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link CachingLookupFunction}. */
class CachingLookupFunctionTest {
    private static final RowData KEY_1 = GenericRowData.of(1);
    private static final Collection<RowData> VALUE_1 =
            Collections.singletonList(GenericRowData.of(1, "Alice", 18L));
    private static final RowData KEY_2 = GenericRowData.of(2);
    private static final Collection<RowData> VALUE_2 =
            Arrays.asList(GenericRowData.of(2, "Bob", 20L), GenericRowData.of(2, "Charlie", 22L));
    private static final RowData NON_EXIST_KEY = GenericRowData.of(3);

    @Test
    void testCaching() throws Exception {
        TestingLookupFunction delegate = new TestingLookupFunction();
        CachingLookupFunction function = createCachingFunction(delegate);

        // All cache miss
        function.lookup(KEY_1);
        function.lookup(KEY_2);
        function.lookup(NON_EXIST_KEY);

        // All cache hit
        function.lookup(KEY_1);
        function.lookup(KEY_2);
        function.lookup(NON_EXIST_KEY);

        assertThat(delegate.getLookupCount()).isEqualTo(3);
        assertThat(function.getCache().getIfPresent(KEY_1))
                .containsExactlyInAnyOrderElementsOf(VALUE_1);
        assertThat(function.getCache().getIfPresent(KEY_2))
                .containsExactlyInAnyOrderElementsOf(VALUE_2);
        assertThat(function.getCache().getIfPresent(NON_EXIST_KEY)).isEmpty();
    }

    private CachingLookupFunction createCachingFunction(LookupFunction delegate) throws Exception {
        CachingLookupFunction function =
                new CachingLookupFunction(
                        DefaultLookupCache.newBuilder().maximumSize(Long.MAX_VALUE).build(),
                        delegate);
        function.open(new FunctionContext(new MockStreamingRuntimeContext(false, 1, 0)));
        return function;
    }

    private static final class TestingLookupFunction extends LookupFunction {
        private final transient Map<RowData, Collection<RowData>> data = new HashMap<>();
        private int lookupCount = 0;

        @Override
        public void open(FunctionContext context) throws Exception {
            data.put(KEY_1, VALUE_1);
            data.put(KEY_2, VALUE_2);
        }

        @Override
        public Collection<RowData> lookup(RowData keyRow) throws IOException {
            lookupCount++;
            return data.get(keyRow);
        }

        public int getLookupCount() {
            return lookupCount;
        }
    }
}
