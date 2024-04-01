package org.apache.flink.formats.protobuf.registry.confluent;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.Assert.assertEquals;

@ExtendWith(TestLoggerExtension.class)
class SchemaRegistryClientFactoryTest {

    @Test
    void testShouldCreateDefaultCoder() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RegistryFormatOptions.SUBJECT,"test-subject");
        configuration.set(RegistryFormatOptions.URL,"localhost");
        configuration.set(RegistryFormatOptions.SCHEMA_CACHE_SIZE,10);
        SchemaCoder coder = SchemaRegistryClientFactory.getCoder(RowType.of(new IntType()),configuration);
        assertEquals(coder.getClass(), SchemaCoderProviders.DefaultSchemaCoder.class);
    }
    @Test
    void testShouldCreatePreRegisteredSchemaCoder() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RegistryFormatOptions.SCHEMA_ID,10);
        configuration.set(RegistryFormatOptions.URL,"localhost");
        configuration.set(RegistryFormatOptions.SCHEMA_CACHE_SIZE,10);
        SchemaCoder coder = SchemaRegistryClientFactory.getCoder(RowType.of(new IntType()),configuration);
        assertEquals(coder.getClass(), SchemaCoderProviders.PreRegisteredSchemaCoder.class);
    }
}
