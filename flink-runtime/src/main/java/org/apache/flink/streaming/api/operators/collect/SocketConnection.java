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

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * {@code SocketConnection} is a helper class to collect Socket-related fields that belong to the
 * same {@link Socket} connection.
 */
class SocketConnection implements AutoCloseable {

    private final Socket socket;
    private final DataInputViewStreamWrapper inStream;
    private final DataOutputViewStreamWrapper outStream;

    public static SocketConnection create(int socketTimeout, InetSocketAddress address)
            throws IOException {
        final Socket newSocket = new Socket();
        newSocket.setSoTimeout(socketTimeout);
        newSocket.setKeepAlive(true);
        newSocket.setTcpNoDelay(true);

        newSocket.connect(address);

        return new SocketConnection(newSocket);
    }

    @VisibleForTesting
    SocketConnection(Socket connectedSocket) throws IOException {
        Preconditions.checkArgument(connectedSocket.isConnected());

        this.socket = connectedSocket;
        this.inStream = new DataInputViewStreamWrapper(socket.getInputStream());
        this.outStream = new DataOutputViewStreamWrapper(socket.getOutputStream());
    }

    public DataInputView getDataInputView() {
        return this.inStream;
    }

    public DataOutputView getDataOutputView() {
        return this.outStream;
    }

    @Override
    public void close() throws Exception {
        this.outStream.close();
        this.inStream.close();
        this.socket.close();
    }
}
