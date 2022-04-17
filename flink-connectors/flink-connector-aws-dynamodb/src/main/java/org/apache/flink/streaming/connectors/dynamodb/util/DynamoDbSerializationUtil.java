package org.apache.flink.streaming.connectors.dynamodb.util;

import org.apache.flink.annotation.Internal;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Serialization Utils for DynamoDb {@link AttributeValue}. This class is currently not
 * serializable, see <a href="https://github.com/aws/aws-sdk-java-v2/issues/3143">open issue</a>
 */
@Internal
public class DynamoDbSerializationUtil {

    public static void serializeWriteRequest(WriteRequest writeRequest, DataOutputStream out)
            throws IOException {
        if (writeRequest.putRequest() != null) {
            out.writeUTF(DynamoDbWriteRequestType.PUT.name());
            Map<String, AttributeValue> item = writeRequest.putRequest().item();
            serializeItem(item, out);
        } else if (writeRequest.deleteRequest() != null) {
            out.writeUTF(DynamoDbWriteRequestType.DELETE.name());
            Map<String, AttributeValue> key = writeRequest.deleteRequest().key();
            serializeItem(key, out);
        } else {
            throw new IllegalArgumentException("Empty write request");
        }
    }

    public static WriteRequest deserializeWriteRequest(DataInputStream in) throws IOException {
        String writeRequestType = in.readUTF();
        DynamoDbWriteRequestType dynamoDbWriteRequestType =
                DynamoDbWriteRequestType.valueOf(writeRequestType);
        switch (dynamoDbWriteRequestType) {
            case PUT:
                return WriteRequest.builder()
                        .putRequest(PutRequest.builder().item(deserializeItem(in)).build())
                        .build();
            case DELETE:
                return WriteRequest.builder()
                        .deleteRequest(DeleteRequest.builder().key(deserializeItem(in)).build())
                        .build();
            default:
                throw new IllegalArgumentException(
                        "Invalid write request type " + writeRequestType);
        }
    }

    public static void serializeItem(Map<String, AttributeValue> item, DataOutputStream out)
            throws IOException {
        out.writeInt(item.size());
        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            out.writeUTF(entry.getKey());
            AttributeValue value = entry.getValue();
            serializeAttributeValue(value, out);
        }
    }

    public static void serializeAttributeValue(AttributeValue value, DataOutputStream out)
            throws IOException {
        if (value.nul() != null) {
            out.writeUTF(DynamoDbType.NULL.name());
        } else if (value.bool() != null) {
            out.writeUTF(DynamoDbType.BOOLEAN.name());
            out.writeBoolean(value.bool());
        } else if (value.s() != null) {
            out.writeUTF(DynamoDbType.STRING.name());
            out.writeUTF(value.s());
        } else if (value.n() != null) {
            out.writeUTF(DynamoDbType.NUMBER.name());
            out.writeUTF(value.n());
        } else if (value.b() != null) {
            out.writeUTF(DynamoDbType.BINARY.name());
            out.writeInt(value.b().asByteArrayUnsafe().length);
            out.write(value.b().asByteArrayUnsafe());
        } else if (value.hasSs()) {
            out.writeUTF(DynamoDbType.STRING_SET.name());
            out.writeInt(value.ss().size());
            for (String s : value.ss()) {
                out.writeUTF(s);
            }
        } else if (value.hasNs()) {
            out.writeUTF(DynamoDbType.NUMBER_SET.name());
            out.writeInt(value.ns().size());
            for (String s : value.ns()) {
                out.writeUTF(s);
            }
        } else if (value.hasBs()) {
            out.writeUTF(DynamoDbType.BINARY_SET.name());
            out.writeInt(value.bs().size());
            for (SdkBytes sdkBytes : value.bs()) {
                byte[] bytes = sdkBytes.asByteArrayUnsafe();
                out.writeInt(bytes.length);
                out.write(bytes);
            }
        } else if (value.hasL()) {
            out.writeUTF(DynamoDbType.LIST.name());
            List<AttributeValue> l = value.l();
            out.writeInt(l.size());
            for (AttributeValue attributeValue : l) {
                serializeAttributeValue(attributeValue, out);
            }
        } else if (value.hasM()) {
            out.writeUTF(DynamoDbType.MAP.name());
            Map<String, AttributeValue> m = value.m();
            serializeItem(m, out);
        } else {
            throw new IllegalArgumentException("Attribute value must not be empty: " + value);
        }
    }

    public static Map<String, AttributeValue> deserializeItem(DataInputStream in)
            throws IOException {
        int size = in.readInt();
        Map<String, AttributeValue> item = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            AttributeValue attributeValue = deserializeAttributeValue(in);
            item.put(key, attributeValue);
        }
        return item;
    }

    public static AttributeValue deserializeAttributeValue(DataInputStream in) throws IOException {
        String type = in.readUTF();
        DynamoDbType dynamoDbType = DynamoDbType.valueOf(type);
        return deserializeAttributeValue(dynamoDbType, in);
    }

    private static AttributeValue deserializeAttributeValue(
            DynamoDbType dynamoDbType, DataInputStream in) throws IOException {
        switch (dynamoDbType) {
            case NULL:
                return AttributeValue.builder().nul(true).build();
            case STRING:
                return AttributeValue.builder().s(in.readUTF()).build();
            case NUMBER:
                return AttributeValue.builder().n(in.readUTF()).build();
            case BOOLEAN:
                return AttributeValue.builder().bool(in.readBoolean()).build();
            case BINARY:
                int length = in.readInt();
                byte[] bytes = new byte[length];
                in.read(bytes);
                return AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build();
            case STRING_SET:
                int stringSetSize = in.readInt();
                Set<String> stringSet = new LinkedHashSet<>(stringSetSize);
                for (int i = 0; i < stringSetSize; i++) {
                    stringSet.add(in.readUTF());
                }
                return AttributeValue.builder().ss(stringSet).build();
            case NUMBER_SET:
                int numberSetSize = in.readInt();
                Set<String> numberSet = new LinkedHashSet<>(numberSetSize);
                for (int i = 0; i < numberSetSize; i++) {
                    numberSet.add(in.readUTF());
                }
                return AttributeValue.builder().ns(numberSet).build();
            case BINARY_SET:
                int binarySetSize = in.readInt();
                Set<SdkBytes> byteSet = new LinkedHashSet<>(binarySetSize);
                for (int i = 0; i < binarySetSize; i++) {
                    int byteLength = in.readInt();
                    byte[] bs = new byte[byteLength];
                    in.read(bs);
                    byteSet.add(SdkBytes.fromByteArray(bs));
                }
                return AttributeValue.builder().bs(byteSet).build();
            case LIST:
                int listSize = in.readInt();
                List<AttributeValue> list = new ArrayList<>(listSize);
                for (int i = 0; i < listSize; i++) {
                    list.add(deserializeAttributeValue(in));
                }
                return AttributeValue.builder().l(list).build();
            case MAP:
                return AttributeValue.builder().m(deserializeItem(in)).build();
            default:
                throw new IllegalArgumentException("Unknown DynamoDbType " + dynamoDbType);
        }
    }
}
