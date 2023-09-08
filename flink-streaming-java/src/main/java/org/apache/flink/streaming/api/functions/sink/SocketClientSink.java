/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.SerializableObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import static org.apache.flink.util.NetUtils.isValidClientPort;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Socket client that acts as a streaming sink. The data is sent to a Socket as a byte array.
 *
 * <p>The sink can be set to retry message sends after the sending failed.
 *
 * <p>The sink can be set to 'autoflush', in which case the socket stream is flushed after every
 * message. This significantly reduced throughput, but also decreases message latency.
 *
 * @param <IN> data to be written into the Socket.
 */
@PublicEvolving
public class SocketClientSink<IN> extends RichSinkFunction<IN> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SocketClientSink.class);

    private static final int CONNECTION_RETRY_DELAY = 500;

    private final SerializableObject lock = new SerializableObject();
    private final SerializationSchema<IN> schema;
    private final String hostName;
    private final int port;
    private final int maxNumRetries;
    private final boolean autoFlush;

    private transient Socket client;
    private transient OutputStream outputStream;

    private int retries;

    private volatile boolean isRunning = true;

    /**
     * Creates a new SocketClientSink. The sink will not attempt to retry connections upon failure
     * and will not auto-flush the stream.
     *
     * @param hostName Hostname of the server to connect to.
     * @param port Port of the server.
     * @param schema Schema used to serialize the data into bytes.
     */
    public SocketClientSink(String hostName, int port, SerializationSchema<IN> schema) {
        this(hostName, port, schema, 0);
    }

    /**
     * Creates a new SocketClientSink that retries connections upon failure up to a given number of
     * times. A value of -1 for the number of retries will cause the system to retry an infinite
     * number of times. The sink will not auto-flush the stream.
     *
     * @param hostName Hostname of the server to connect to.
     * @param port Port of the server.
     * @param schema Schema used to serialize the data into bytes.
     * @param maxNumRetries The maximum number of retries after a message send failed.
     */
    public SocketClientSink(
            String hostName, int port, SerializationSchema<IN> schema, int maxNumRetries) {
        this(hostName, port, schema, maxNumRetries, false);
    }

    /**
     * Creates a new SocketClientSink that retries connections upon failure up to a given number of
     * times. A value of -1 for the number of retries will cause the system to retry an infinite
     * number of times.
     *
     * @param hostName Hostname of the server to connect to.
     * @param port Port of the server.
     * @param schema Schema used to serialize the data into bytes.
     * @param maxNumRetries The maximum number of retries after a message send failed.
     * @param autoflush Flag to indicate whether the socket stream should be flushed after each
     *     message.
     */
    public SocketClientSink(
            String hostName,
            int port,
            SerializationSchema<IN> schema,
            int maxNumRetries,
            boolean autoflush) {
        checkArgument(isValidClientPort(port), "port is out of range");
        checkArgument(
                maxNumRetries >= -1,
                "maxNumRetries must be zero or larger (num retries), or -1 (infinite retries)");

        this.hostName = checkNotNull(hostName, "hostname must not be null");
        this.port = port;
        this.schema = checkNotNull(schema);
        this.maxNumRetries = maxNumRetries;
        this.autoFlush = autoflush;
    }

    // ------------------------------------------------------------------------
    //  Life cycle
    // ------------------------------------------------------------------------

    /**
     * Initialize the connection with the Socket in the server.
     *
     * @param openContext the context.
     */
    @Override
    public void open(OpenContext openContext) throws Exception {
        try {
            synchronized (lock) {
                createConnection();
            }
        } catch (IOException e) {
            throw new IOException("Cannot connect to socket server at " + hostName + ":" + port, e);
        }
    }

    /**
     * Called when new data arrives to the sink, and forwards it to Socket.
     *
     * @param value The value to write to the socket.
     */
    @Override
    public void invoke(IN value) throws Exception {
        byte[] msg = schema.serialize(value);

        try {
            outputStream.write(msg);
            if (autoFlush) {
                outputStream.flush();
            }
        } catch (IOException e) {
            // if no re-tries are enable, fail immediately
            if (maxNumRetries == 0) {
                throw new IOException(
                        "Failed to send message '"
                                + value
                                + "' to socket server at "
                                + hostName
                                + ":"
                                + port
                                + ". Connection re-tries are not enabled.",
                        e);
            }

            LOG.error(
                    "Failed to send message '"
                            + value
                            + "' to socket server at "
                            + hostName
                            + ":"
                            + port
                            + ". Trying to reconnect...",
                    e);

            // do the retries in locked scope, to guard against concurrent close() calls
            // note that the first re-try comes immediately, without a wait!

            synchronized (lock) {
                IOException lastException = null;
                retries = 0;

                while (isRunning && (maxNumRetries < 0 || retries < maxNumRetries)) {

                    // first, clean up the old resources
                    try {
                        if (outputStream != null) {
                            outputStream.close();
                        }
                    } catch (IOException ee) {
                        LOG.error("Could not close output stream from failed write attempt", ee);
                    }
                    try {
                        if (client != null) {
                            client.close();
                        }
                    } catch (IOException ee) {
                        LOG.error("Could not close socket from failed write attempt", ee);
                    }

                    // try again
                    retries++;

                    try {
                        // initialize a new connection
                        createConnection();

                        // re-try the write
                        outputStream.write(msg);

                        // success!
                        return;
                    } catch (IOException ee) {
                        lastException = ee;
                        LOG.error(
                                "Re-connect to socket server and send message failed. Retry time(s): "
                                        + retries,
                                ee);
                    }

                    // wait before re-attempting to connect
                    lock.wait(CONNECTION_RETRY_DELAY);
                }

                // throw an exception if the task is still running, otherwise simply leave the
                // method
                if (isRunning) {
                    throw new IOException(
                            "Failed to send message '"
                                    + value
                                    + "' to socket server at "
                                    + hostName
                                    + ":"
                                    + port
                                    + ". Failed after "
                                    + retries
                                    + " retries.",
                            lastException);
                }
            }
        }
    }

    /** Closes the connection with the Socket server. */
    @Override
    public void close() throws Exception {
        // flag this as not running any more
        isRunning = false;

        // clean up in locked scope, so there is no concurrent change to the stream and client
        synchronized (lock) {
            // we notify first (this statement cannot fail). The notified thread will not continue
            // anyways before it can re-acquire the lock
            lock.notifyAll();

            try {
                if (outputStream != null) {
                    outputStream.close();
                }
            } finally {
                if (client != null) {
                    client.close();
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private void createConnection() throws IOException {
        client = new Socket(hostName, port);
        client.setKeepAlive(true);
        client.setTcpNoDelay(true);

        outputStream = client.getOutputStream();
    }

    // ------------------------------------------------------------------------
    //  For testing
    // ------------------------------------------------------------------------

    int getCurrentNumberOfRetries() {
        synchronized (lock) {
            return retries;
        }
    }
}
