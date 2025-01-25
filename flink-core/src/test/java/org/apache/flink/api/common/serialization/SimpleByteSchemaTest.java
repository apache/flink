package org.apache.flink.api.common.serialization;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleByteSchemaTest {
    @Test
    void testSimpleSerialisation() throws IOException {
        final byte[] testBytes = "hello world".getBytes();
        assertThat(new SimpleByteSchema().serialize(testBytes)).isEqualTo(testBytes);
        assertThat(new SimpleByteSchema().deserialize(testBytes)).isEqualTo(testBytes);
    }
}
