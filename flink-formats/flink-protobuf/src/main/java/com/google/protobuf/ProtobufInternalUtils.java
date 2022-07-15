package com.google.protobuf;

public class ProtobufInternalUtils {
    /**
     * convert under score name to camel name
     *
     * @param name
     * @param capNext
     * @return
     */
    public static String underScoreToCamelCase(String name, boolean capNext) {
        return SchemaUtil.toCamelCase(name, capNext);
    }
}
