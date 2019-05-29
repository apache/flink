package org.apache.flink.formats.thrift;

/**
 *  Enum for thrift code generators. Thrift code generation among generators varies. scrooge-maven-plugin
 *  generates code that treats binary as string.  maven-thrift-plugin generates code that use ByteBuffer
 *  to represent binary type.
 */
public enum ThriftCodeGenerator {
	THRIFT,
	SCROOGE
}
