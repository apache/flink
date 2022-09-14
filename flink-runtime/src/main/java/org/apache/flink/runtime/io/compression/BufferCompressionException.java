/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.compression;

/**
 * A {@code BufferCompressionException} is thrown when the target data cannot be compressed, such as
 * insufficient target buffer space for compression, etc.
 */
public class BufferCompressionException extends RuntimeException {

    public BufferCompressionException() {
        super();
    }

    public BufferCompressionException(String message) {
        super(message);
    }

    public BufferCompressionException(String message, Throwable e) {
        super(message, e);
    }

    public BufferCompressionException(Throwable e) {
        super(e);
    }
}
