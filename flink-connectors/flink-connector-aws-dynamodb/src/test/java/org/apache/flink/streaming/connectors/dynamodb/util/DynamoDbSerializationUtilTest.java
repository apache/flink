package org.apache.flink.streaming.connectors.dynamodb.util;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/** Tests for {@link DynamoDbSerializationUtil}. */
public class DynamoDbSerializationUtilTest {

    @Test
    public void testPutItemSerializeDeserialize() throws IOException {
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put("string", AttributeValue.builder().s("string").build());
        item.put("number", AttributeValue.builder().s("123.4212").build());
        item.put(
                "binary",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 3})).build());
        item.put("null", AttributeValue.builder().nul(true).build());
        item.put("boolean", AttributeValue.builder().bool(true).build());
        item.put("stringSet", AttributeValue.builder().ss("s1", "s2").build());
        item.put("numberSet", AttributeValue.builder().ns("123.1231", "123123").build());
        item.put(
                "binarySet",
                AttributeValue.builder()
                        .bs(
                                SdkBytes.fromByteArray(new byte[] {1, 2, 3}),
                                SdkBytes.fromByteArray(new byte[] {4, 5, 6}))
                        .build());
        item.put(
                "list",
                AttributeValue.builder()
                        .l(
                                AttributeValue.builder().s("s1").build(),
                                AttributeValue.builder().s("s2").build())
                        .build());
        item.put(
                "map",
                AttributeValue.builder()
                        .m(
                                ImmutableMap.of(
                                        "key1", AttributeValue.builder().s("string").build(),
                                        "key2", AttributeValue.builder().n("12345").build(),
                                        "binary",
                                                AttributeValue.builder()
                                                        .b(
                                                                SdkBytes.fromByteArray(
                                                                        new byte[] {1, 2, 3}))
                                                        .build(),
                                        "null", AttributeValue.builder().nul(true).build()))
                        .build());
        WriteRequest writeRequest =
                WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(outputStream);
        DynamoDbSerializationUtil.serializeWriteRequest(writeRequest, out);
        outputStream.close();
        out.close();
        InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        WriteRequest deserializedWriteRequest =
                DynamoDbSerializationUtil.deserializeWriteRequest(dataInputStream);
        inputStream.close();
        assertThat(deserializedWriteRequest).isEqualTo(writeRequest);
    }

    @Test
    public void testDeleteItemSerializeDeserialize() throws IOException {
        final Map<String, AttributeValue> key = new HashMap<>();
        key.put("string", AttributeValue.builder().s("string").build());
        key.put("number", AttributeValue.builder().s("123.4212").build());
        WriteRequest writeRequest =
                WriteRequest.builder()
                        .deleteRequest(DeleteRequest.builder().key(key).build())
                        .build();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(outputStream);
        DynamoDbSerializationUtil.serializeWriteRequest(writeRequest, out);
        outputStream.close();
        out.close();
        InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        WriteRequest deserializedWriteRequest =
                DynamoDbSerializationUtil.deserializeWriteRequest(dataInputStream);
        inputStream.close();
        assertThat(deserializedWriteRequest).isEqualTo(writeRequest);
    }

    @Test
    public void whenSerializeEmptyWriteRequestShouldThrowIllegalArgumentException()
            throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(outputStream)) {
            assertThatIllegalArgumentException()
                    .isThrownBy(
                            () ->
                                    DynamoDbSerializationUtil.serializeWriteRequest(
                                            WriteRequest.builder().build(), out));
        }
    }
}
