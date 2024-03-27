package org.apache.flink.formats.protobuf.registry.confluent;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import io.confluent.kafka.schemaregistry.ParsedSchema;


public interface SchemaCoder {
    ParsedSchema readSchema(InputStream in) throws IOException;

    default void initialize(){

    }

    void writeSchema(OutputStream out) throws IOException;

}
