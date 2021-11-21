package org.apache.flink.mongodb.streaming.sink;

import org.bson.Document;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link DocumentBulkSerializer}.
 **/
public class DocumentBulkSerializerTest {

    @Test
    public void testSerde() throws IOException {
        String json =
                "{\n" +
                "\"name\": \"A Brief History of Time\",\n" +
                "\"author\": \"Stephen Hawking\",\n" +
                "\"language\": \"English\",\n" +
                "\"publication year\": 1988\n" +
                "}";
        Document document = Document.parse(json);
        DocumentBulk origin = new DocumentBulk();
        origin.add(document);
        DocumentBulkSerializer serializer = new DocumentBulkSerializer();
        byte[] bytes = serializer.serialize(origin);
        assertEquals(origin, serializer.deserialize(serializer.getVersion(), bytes));
    }
}
