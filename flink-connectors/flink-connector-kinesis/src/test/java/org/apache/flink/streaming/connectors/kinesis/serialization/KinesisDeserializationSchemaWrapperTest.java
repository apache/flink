package org.apache.flink.streaming.connectors.kinesis.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class KinesisDeserializationSchemaWrapperTest {

    @Test
    public void testKinesisConsumerRecognizesSchemaWithCollector() {
        DeserializationSchema<Object> schemaWithCollector =
                new DeserializationSchema<Object>() {
                    @Override
                    public Object deserialize(byte[] message) throws IOException {
                        return null;
                    }

                    @Override
                    public void deserialize(byte[] message, Collector<Object> out)
                            throws IOException {
                        // we do not care about the implementation. we should just check if this
                        // method is declared
                    }

                    @Override
                    public boolean isEndOfStream(Object nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<Object> getProducedType() {
                        return null;
                    }
                };
        KinesisDeserializationSchemaWrapper<Object> wrapper =
                new KinesisDeserializationSchemaWrapper(schemaWithCollector);
        Assert.assertTrue(wrapper.isUseCollector());
    }

    @Test
    public void testKinesisConsumerRecognizesSchemaWithoutCollector() {
        DeserializationSchema<Object> schemaWithCollector =
                new DeserializationSchema<Object>() {
                    @Override
                    public Object deserialize(byte[] message) throws IOException {
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(Object nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<Object> getProducedType() {
                        return null;
                    }
                };
        KinesisDeserializationSchemaWrapper<Object> wrapper =
                new KinesisDeserializationSchemaWrapper(schemaWithCollector);
        Assert.assertFalse(wrapper.isUseCollector());
    }
}
