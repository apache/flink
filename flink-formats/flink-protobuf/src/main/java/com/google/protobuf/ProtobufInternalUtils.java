package com.google.protobuf;

public class ProtobufInternalUtils {
    public static String toCamelCase(String name, boolean capNext) {
        return SchemaUtil.toCamelCase(name, capNext);
    }
}
