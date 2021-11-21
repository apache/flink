package org.apache.flink.mongodb.streaming.serde;

import org.bson.Document;

import java.io.IOException;
import java.io.Serializable;

public interface DocumentDeserializer<T> extends Serializable {

    /**
     * Serialize input Java objects into {@link Document}.
     *
     * @param document The input {@link Document}.
     * @return The serialized object.
     */
    T deserialize(Document document) throws IOException;

}
