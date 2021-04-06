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

package org.apache.flink.runtime.blob;

import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Implements a {@link BlobServer} that, after some initial normal operation, closes incoming
 * connections for a given number of times and continues normally again.
 */
public class TestingFailingBlobServer extends BlobServer {

    private final int numAccept;
    private final int numFailures;

    public TestingFailingBlobServer(Configuration config, BlobStore blobStore, int numFailures)
            throws IOException {
        this(config, blobStore, 1, numFailures);
    }

    public TestingFailingBlobServer(
            Configuration config, BlobStore blobStore, int numAccept, int numFailures)
            throws IOException {
        super(config, blobStore);
        this.numAccept = numAccept;
        this.numFailures = numFailures;
    }

    @Override
    public void run() {

        // we do properly the first operation (PUT)
        try {
            for (int num = 0; num < numAccept && !isShutdown(); num++) {
                new BlobServerConnection(getServerSocket().accept(), this).start();
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }

        // do some failing operations
        for (int num = 0; num < numFailures && !isShutdown(); num++) {
            Socket socket = null;
            try {
                socket = getServerSocket().accept();
                InputStream is = socket.getInputStream();
                OutputStream os = socket.getOutputStream();

                // just abort everything
                is.close();
                os.close();
                socket.close();
            } catch (IOException ignored) {
            } finally {
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (Throwable ignored) {
                    }
                }
            }
        }

        // regular runs
        super.run();
    }
}
