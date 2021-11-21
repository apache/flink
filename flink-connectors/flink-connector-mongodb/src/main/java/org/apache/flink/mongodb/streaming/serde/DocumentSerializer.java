package org.apache.flink.mongodb.streaming.serde;

import org.bson.Document;

import java.io.Serializable;

public interface DocumentSerializer<T> extends Serializable {

    /**
     * Serialize input Java objects into {@link Document}.
     *
     * @param object The input object.
     * @return The serialized {@link Document}.
     */
    Document serialize(T object);
}
