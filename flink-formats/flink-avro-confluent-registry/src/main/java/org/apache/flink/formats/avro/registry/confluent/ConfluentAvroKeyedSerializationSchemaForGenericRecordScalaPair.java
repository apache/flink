package org.apache.flink.formats.avro.registry.confluent;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serialization schema that serializes instances of key-value Scala pairs of {@link GenericRecord} to Avro binary format using {@link KafkaAvroSerializer} that uses
 * Confluent Schema Registry.
 */
public class ConfluentAvroKeyedSerializationSchemaForGenericRecordScalaPair
        implements KeyedSerializationSchema<Tuple2<GenericRecord, GenericRecord>> {


    private final String topic;
    private final File valueSchemaFile;
    private final File keySchemaFile;
    private final String registryURL;
    private transient Schema valueSchema;
    private transient Schema keySchema;

    private transient KafkaAvroSerializer valueSerializer;
    private transient KafkaAvroSerializer KeySerializer;

    /**
     * Creates a keyed Avro serialization schema.
     * The constructor takes files instead of {@link Schema} because {@link Schema} is not serializable.
     *
     * @param topic             Kafka topic to write to
     * @param registryURL       url of schema registry to connect
     * @param keySchemaFile     file of the Avro writer schema used by the key
     * @param valueSchemaFile   file of the Avro writer schema used by the value
     */
    public ConfluentAvroKeyedSerializationSchemaForGenericRecordScalaPair(String topic, String registryURL, File keySchemaFile, File valueSchemaFile) {
        this.topic = topic;
        this.registryURL =registryURL;
        this.valueSchemaFile = valueSchemaFile;
        this.keySchemaFile = keySchemaFile;
    }

    /**
     * Optional method to determine the target topic for the element.
     *
     * @param key_value input Scala pair of key and value
     * @return null or the target topic
     */
    @Override
    public String getTargetTopic(Tuple2<GenericRecord, GenericRecord> key_value) {
        return topic;
    }

    /**
     * Serializes the key of the input Scala pair of key and value.
     *
     * @param key_value input Scala pair of key and value
     * @return          byte array of the serialized key
     */
    @Override
    public byte[] serializeKey(Tuple2<GenericRecord, GenericRecord> key_value) {
        if (this.KeySerializer == null) {
            Schema.Parser parser = new Schema.Parser();
            try {
                this.keySchema = parser.parse(keySchemaFile);
            } catch (IOException e) {
                throw new IllegalArgumentException("Cannot parse key Avro writer schema file: " + keySchemaFile, e);
            }
            CachedSchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(registryURL, 100);
            this.KeySerializer = new KafkaAvroSerializer(schemaRegistry);
            Map map = new HashMap();
            map.put("schema.registry.url", registryURL);
            this.KeySerializer.configure(map, true);
        }
        List<Schema.Field> filds = keySchema.getFields();
        GenericRecord reconstructedRecord = new GenericData.Record(keySchema);
        for(int i=0; i<filds.size(); i++) {
            String key = filds.get(i).name();
            reconstructedRecord.put(key, key_value._1.get(key));
        }
        return KeySerializer.serialize(topic, reconstructedRecord);
    }

    /**
     * Serializes the value of the input Scala pair of key and value.
     *
     * @param key_value input Scala pair of key and value
     * @return          byte array of the serialized value
     */
    @Override
    public byte[] serializeValue(Tuple2<GenericRecord, GenericRecord> key_value
    ) {
        if (this.valueSerializer == null) {
            Schema.Parser parser = new Schema.Parser();
            try {
                this.valueSchema = parser.parse(valueSchemaFile);
            } catch (IOException e) {
                throw new IllegalArgumentException("Cannot parse value Avro writer schema file: " + valueSchemaFile, e);
            }
            CachedSchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(registryURL, 500);
            this.valueSerializer = new KafkaAvroSerializer(schemaRegistry);
        }
        List<Schema.Field> filds = valueSchema.getFields();
        GenericRecord reconstructedRecord = new GenericData.Record(valueSchema);
        for(int i=0; i<filds.size(); i++) {
            String key = filds.get(i).name();
            reconstructedRecord.put(key, key_value._2.get(key));
        }
        return valueSerializer.serialize(topic, reconstructedRecord);
    }

}
