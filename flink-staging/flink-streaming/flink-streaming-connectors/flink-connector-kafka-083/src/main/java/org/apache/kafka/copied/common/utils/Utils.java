/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.copied.common.utils;

import org.apache.kafka.copied.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {

    // This matches URIs of formats: host:port and protocol:\\host:port
    // IPv6 is supported with [ip] pattern
    private static final Pattern HOST_PORT_PATTERN = Pattern.compile(".*?\\[?([0-9a-z\\-.:]*)\\]?:([0-9]+)");

    public static final String NL = System.getProperty("line.separator");

    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    /**
     * Turn the given UTF8 byte array into a string
     * 
     * @param bytes The byte array
     * @return The string
     */
    public static String utf8(byte[] bytes) {
        try {
            return new String(bytes, "UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("This shouldn't happen.", e);
        }
    }

    /**
     * Turn a string into a utf8 byte[]
     * 
     * @param string The string
     * @return The byte[]
     */
    public static byte[] utf8(String string) {
        try {
            return string.getBytes("UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("This shouldn't happen.", e);
        }
    }

    /**
     * Read an unsigned integer from the current position in the buffer, incrementing the position by 4 bytes
     * 
     * @param buffer The buffer to read from
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer) {
        return buffer.getInt() & 0xffffffffL;
    }

    /**
     * Read an unsigned integer from the given position without modifying the buffers position
     * 
     * @param buffer the buffer to read from
     * @param index the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer, int index) {
        return buffer.getInt(index) & 0xffffffffL;
    }

    /**
     * Read an unsigned integer stored in little-endian format from the {@link InputStream}.
     * 
     * @param in The stream to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    public static int readUnsignedIntLE(InputStream in) throws IOException {
        return (in.read() << 8 * 0) 
             | (in.read() << 8 * 1)
             | (in.read() << 8 * 2)
             | (in.read() << 8 * 3);
    }

    /**
     * Read an unsigned integer stored in little-endian format from a byte array
     * at a given offset.
     * 
     * @param buffer The byte array to read from
     * @param offset The position in buffer to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    public static int readUnsignedIntLE(byte[] buffer, int offset) {
        return (buffer[offset++] << 8 * 0)
             | (buffer[offset++] << 8 * 1)
             | (buffer[offset++] << 8 * 2)
             | (buffer[offset]   << 8 * 3);
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     * 
     * @param buffer The buffer to write to
     * @param value The value to write
     */
    public static void writetUnsignedInt(ByteBuffer buffer, long value) {
        buffer.putInt((int) (value & 0xffffffffL));
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     * 
     * @param buffer The buffer to write to
     * @param index The position in the buffer at which to begin writing
     * @param value The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, int index, long value) {
        buffer.putInt(index, (int) (value & 0xffffffffL));
    }

    /**
     * Write an unsigned integer in little-endian format to the {@link OutputStream}.
     * 
     * @param out The stream to write to
     * @param value The value to write
     */
    public static void writeUnsignedIntLE(OutputStream out, int value) throws IOException {
        out.write(value >>> 8 * 0);
        out.write(value >>> 8 * 1);
        out.write(value >>> 8 * 2);
        out.write(value >>> 8 * 3);
    }

    /**
     * Write an unsigned integer in little-endian format to a byte array
     * at a given offset.
     * 
     * @param buffer The byte array to write to
     * @param offset The position in buffer to write to
     * @param value The value to write
     */
    public static void writeUnsignedIntLE(byte[] buffer, int offset, int value) {
        buffer[offset++] = (byte) (value >>> 8 * 0);
        buffer[offset++] = (byte) (value >>> 8 * 1);
        buffer[offset++] = (byte) (value >>> 8 * 2);
        buffer[offset]   = (byte) (value >>> 8 * 3);
    }


    /**
     * Get the absolute value of the given number. If the number is Int.MinValue return 0. This is different from
     * java.lang.Math.abs or scala.math.abs in that they return Int.MinValue (!).
     */
    public static int abs(int n) {
        return (n == Integer.MIN_VALUE) ? 0 : Math.abs(n);
    }

    /**
     * Get the minimum of some long values.
     * @param first Used to ensure at least one value
     * @param rest The rest of longs to compare
     * @return The minimum of all passed argument.
     */
    public static long min(long first, long ... rest) {
        long min = first;
        for (int i = 0; i < rest.length; i++) {
            if (rest[i] < min)
                min = rest[i];
        }
        return min;
    }

    /**
     * Get the length for UTF8-encoding a string without encoding it first
     * 
     * @param s The string to calculate the length for
     * @return The length when serialized
     */
    public static int utf8Length(CharSequence s) {
        int count = 0;
        for (int i = 0, len = s.length(); i < len; i++) {
            char ch = s.charAt(i);
            if (ch <= 0x7F) {
                count++;
            } else if (ch <= 0x7FF) {
                count += 2;
            } else if (Character.isHighSurrogate(ch)) {
                count += 4;
                ++i;
            } else {
                count += 3;
            }
        }
        return count;
    }

    /**
     * Read the given byte buffer into a byte array
     */
    public static byte[] toArray(ByteBuffer buffer) {
        return toArray(buffer, 0, buffer.limit());
    }

    /**
     * Read a byte array from the given offset and size in the buffer
     */
    public static byte[] toArray(ByteBuffer buffer, int offset, int size) {
        byte[] dest = new byte[size];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.arrayOffset() + offset, dest, 0, size);
        } else {
            int pos = buffer.position();
            buffer.get(dest);
            buffer.position(pos);
        }
        return dest;
    }

    /**
     * Check that the parameter t is not null
     * 
     * @param t The object to check
     * @return t if it isn't null
     * @throws NullPointerException if t is null.
     */
    public static <T> T notNull(T t) {
        if (t == null)
            throw new NullPointerException();
        else
            return t;
    }

    /**
     * Sleep for a bit
     * @param ms The duration of the sleep
     */
    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // this is okay, we just wake up early
        }
    }

    /**
     * Instantiate the class
     */
    public static Object newInstance(Class<?> c) {
        try {
            return c.newInstance();
        } catch (IllegalAccessException e) {
            throw new KafkaException("Could not instantiate class " + c.getName(), e);
        } catch (InstantiationException e) {
            throw new KafkaException("Could not instantiate class " + c.getName() + " Does it have a public no-argument constructor?", e);
        }
    }

    /**
     * Generates 32 bit murmur2 hash from byte array
     * @param data byte array to hash
     * @return 32 bit hash of the given array
     */
    public static int murmur2(final byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= data[length & ~3] & 0xff;
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    /**
     * Extracts the hostname from a "host:port" address string.
     * @param address address string to parse
     * @return hostname or null if the given address is incorrect
     */
    public static String getHost(String address) {
        Matcher matcher = HOST_PORT_PATTERN.matcher(address);
        return matcher.matches() ? matcher.group(1) : null;
    }

    /**
     * Extracts the port number from a "host:port" address string.
     * @param address address string to parse
     * @return port number or null if the given address is incorrect
     */
    public static Integer getPort(String address) {
        Matcher matcher = HOST_PORT_PATTERN.matcher(address);
        return matcher.matches() ? Integer.parseInt(matcher.group(2)) : null;
    }

    /**
     * Formats hostname and port number as a "host:port" address string,
     * surrounding IPv6 addresses with braces '[', ']'
     * @param host hostname
     * @param port port number
     * @return address string
     */
    public static String formatAddress(String host, Integer port) {
        return host.contains(":")
                ? "[" + host + "]:" + port // IPv6
                : host + ":" + port;
    }

    /**
     * Create a string representation of an array joined by the given separator
     * @param strs The array of items
     * @param seperator The separator
     * @return The string representation.
     */
    public static <T> String join(T[] strs, String seperator) {
        return join(Arrays.asList(strs), seperator);
    }
    
    /**
     * Create a string representation of a list joined by the given separator
     * @param list The list of items
     * @param seperator The separator
     * @return The string representation.
     */
    public static <T> String join(Collection<T> list, String seperator) {
        StringBuilder sb = new StringBuilder();
        Iterator<T> iter = list.iterator();
        while (iter.hasNext()) {
            sb.append(iter.next());
            if (iter.hasNext())
                sb.append(seperator);  
        }
        return sb.toString();
    }

    /**
     * Read a properties file from the given path
     * @param filename The path of the file to read
     */
    public static Properties loadProps(String filename) throws IOException, FileNotFoundException {
        Properties props = new Properties();
        InputStream propStream = null;
        try {
            propStream = new FileInputStream(filename);
            props.load(propStream);
        } finally {
            if (propStream != null)
                propStream.close();
        }
        return props;
    }

    /**
     * Get the stack trace from an exception as a string
     */
    public static String stackTrace(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    /**
     * Create a new thread
     * @param name The name of the thread
     * @param runnable The work for the thread to do
     * @param daemon Should the thread block JVM shutdown?
     * @return The unstarted thread
     */
    public static Thread newThread(String name, Runnable runnable, Boolean daemon) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(daemon);
        thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                log.error("Uncaught exception in thread '" + t.getName() + "':", e);
            }
        });
        return thread;
    }

    /**
     * Create a daemon thread
     * @param name The name of the thread
     * @param runnable The runnable to execute in the background
     * @return The unstarted thread
     */
    public static Thread daemonThread(String name, Runnable runnable) {
        return newThread(name, runnable, true);
    }

    /**
     * Print an error message and shutdown the JVM
     * @param message The error message
     */
    public static void croak(String message) {
        System.err.println(message);
        System.exit(1);
    }

    /**
     * Read a buffer into a Byte array for the given offset and length
     */
    public static byte[] readBytes(ByteBuffer buffer, int offset, int length) {
        byte[] dest = new byte[length];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.arrayOffset() + offset, dest, 0, length);
        } else {
            buffer.mark();
            buffer.position(offset);
            buffer.get(dest, 0, length);
            buffer.reset();
        }
        return dest;
    }

    /**
     * Read the given byte buffer into a Byte array
     */
    public static byte[] readBytes(ByteBuffer buffer) {
        return Utils.readBytes(buffer, 0, buffer.limit());
    }

    /**
     * Attempt to read a file as a string
     * @throws IOException 
     */
    public static String readFileAsString(String path, Charset charset) throws IOException {
        if (charset == null) charset = Charset.defaultCharset();
        FileInputStream stream = new FileInputStream(new File(path));
        String result = new String();
        try {
            FileChannel fc = stream.getChannel();
            MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
            result = charset.decode(bb).toString();
        } finally {
            stream.close();
        }
        return result;
    }

    public static String readFileAsString(String path) throws IOException {
        return Utils.readFileAsString(path, Charset.defaultCharset());
    }
}
