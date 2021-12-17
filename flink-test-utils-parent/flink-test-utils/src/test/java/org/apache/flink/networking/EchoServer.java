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

package org.apache.flink.networking;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** TCP EchoServer for test purposes. */
public class EchoServer extends Thread implements AutoCloseable {
    private final ServerSocket serverSocket = new ServerSocket(0);
    private final int socketTimeout;
    private final List<EchoWorkerThread> workerThreads =
            Collections.synchronizedList(new ArrayList<>());

    private volatile boolean close = false;
    private Exception threadException;

    public EchoServer(int socketTimeout) throws IOException {
        serverSocket.setSoTimeout(socketTimeout);
        this.socketTimeout = socketTimeout;
    }

    public int getLocalPort() {
        return serverSocket.getLocalPort();
    }

    @Override
    public void run() {
        while (!close) {
            try {
                // We are NOT using NetUtils.acceptWithoutTimeout here as this ServerSocket sets
                // a timeout.
                EchoWorkerThread thread =
                        new EchoWorkerThread(serverSocket.accept(), socketTimeout);
                thread.start();
            } catch (IOException e) {
                threadException = e;
            }
        }
    }

    @Override
    public void close() throws Exception {
        for (EchoWorkerThread thread : workerThreads) {
            thread.close();
            thread.join();
        }
        close = true;
        if (threadException != null) {
            throw threadException;
        }
        serverSocket.close();
        this.join();
    }

    private static class EchoWorkerThread extends Thread implements AutoCloseable {
        private final PrintWriter output;
        private final BufferedReader input;

        private volatile boolean close;
        private Exception threadException;

        public EchoWorkerThread(Socket clientSocket, int socketTimeout) throws IOException {
            output = new PrintWriter(clientSocket.getOutputStream(), true);
            input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            clientSocket.setSoTimeout(socketTimeout);
        }

        @Override
        public void run() {
            try {
                String inputLine;
                while (!close && (inputLine = input.readLine()) != null) {
                    output.println(inputLine);
                }
            } catch (IOException e) {
                threadException = e;
            }
        }

        @Override
        public void close() throws Exception {
            close = true;
            if (threadException != null) {
                throw threadException;
            }
            input.close();
            output.close();
            this.join();
        }
    }
}
