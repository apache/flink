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

package org.apache.flink.runtime.net;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;

import java.io.IOException;
import java.net.InetAddress;

public class SSLSocketServerTest extends Thread {

    SSLServerSocket serverSocket;

    public SSLSocketServerTest(ReloadableSslContext reloadableSslContext) throws IOException {
        SSLServerSocketFactory factory =
                reloadableSslContext.getSSLContext().getServerSocketFactory();
        serverSocket =
                (SSLServerSocket) factory.createServerSocket(0, 0, InetAddress.getByName(null));
        serverSocket.setNeedClientAuth(false);
    }

    @Override
    public void run() {
        try {
            SSLSocket socket = (SSLSocket) serverSocket.accept();
            socket.getOutputStream().write("Hello World".getBytes());
        } catch (IOException ignored) {
        }
    }

    public int getLocalPort() {
        return serverSocket.getLocalPort();
    }

    public void close() throws IOException {
        serverSocket.close();
    }
}
