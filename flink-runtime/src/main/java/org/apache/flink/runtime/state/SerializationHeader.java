///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.flink.runtime.state;
//
//import org.apache.flink.core.io.VersionedIOReadableWritable;
//import org.apache.flink.core.memory.DataInputView;
//import org.apache.flink.core.memory.DataOutputView;
//import org.apache.flink.util.Preconditions;
//
//import java.io.IOException;
//
//public class SerializationHeader extends VersionedIOReadableWritable {
//
//	private static final long BASIC_HEADER_VERSION = 1L;
//	private static final long BASIC_HEADER_TYPE = 1L;
//	private static final long BASIC_HEADER_SIZE = 3 * Long.SIZE / Byte.SIZE;
//
//	private long size;
//	private long type;
//	private long version;
//
//	public SerializationHeader() {
//		this(BASIC_HEADER_TYPE, BASIC_HEADER_VERSION, BASIC_HEADER_SIZE);
//	}
//
//	public SerializationHeader(long type, long version, long size) {
//		Preconditions.checkArgument(size >= 0);
//		this.type = type;
//		this.size = size;
//		this.version = version;
//	}
//
//	public long getType() {
//		return type;
//	}
//
//	public void setType(long type) {
//		this.type = type;
//	}
//
//	public long getSize() {
//		return size;
//	}
//
//	public void setSize(long size) {
//		this.size = size;
//	}
//
//	public void setVersion(long version) {
//		this.version = version;
//	}
//
//	@Override
//	public long getVersion() {
//		return version;
//	}
//
//	@Override
//	public boolean isCompatibleVersion(long version) {
//		return this.version == version;
//	}
//
//	@Override
//	protected void checkFoundVersion(long foundVersion) throws IOException {
//		setVersion(foundVersion);
//		super.checkFoundVersion(foundVersion);
//	}
//
//	@Override
//	public void write(DataOutputView out) throws IOException {
//		out.writeLong(getSize());
//		out.writeLong(getType());
//		super.write(out);
//	}
//
//	@Override
//	public void read(DataInputView in) throws IOException {
//		setSize(in.readLong());
//		setType(in.readLong());
//		super.read(in);
//	}
//}