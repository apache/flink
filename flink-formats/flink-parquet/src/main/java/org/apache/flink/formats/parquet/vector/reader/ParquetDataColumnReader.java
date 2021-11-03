/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet.vector.reader;

import org.apache.flink.table.data.TimestampData;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.Dictionary;

import java.io.IOException;

/**
 * The interface to wrap the underlying Parquet dictionary and non dictionary encoded page reader.
 */
public interface ParquetDataColumnReader {

    /**
     * Initialize the reader by page data.
     *
     * @param valueCount value count
     * @param in page data
     * @throws IOException
     */
    void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException;

    /** @return the next Dictionary ID from the page */
    int readValueDictionaryId();

    /** @return the next Long from the page */
    long readLong();

    /** @return the next Integer from the page */
    int readInteger();

    /** @return the next SmallInt from the page */
    int readSmallInt();

    /** @return the next TinyInt from the page */
    int readTinyInt();

    /** @return the next Float from the page */
    float readFloat();

    /** @return the next Boolean from the page */
    boolean readBoolean();

    /** @return the next String from the page */
    byte[] readString();

    /** @return the next Varchar from the page */
    byte[] readVarchar();

    /** @return the next Char from the page */
    byte[] readChar();

    /** @return the next Bytes from the page */
    byte[] readBytes();

    /** @return the next Decimal from the page */
    byte[] readDecimal();

    /** @return the next Double from the page */
    double readDouble();

    /** @return the next TimestampData from the page */
    TimestampData readTimestamp();

    /** @return is data valid */
    boolean isValid();

    /** @return the underlying dictionary if current reader is dictionary encoded */
    Dictionary getDictionary();

    /**
     * @param id in dictionary
     * @return the Bytes from the dictionary by id
     */
    byte[] readBytes(int id);

    /**
     * @param id in dictionary
     * @return the Float from the dictionary by id
     */
    float readFloat(int id);

    /**
     * @param id in dictionary
     * @return the Double from the dictionary by id
     */
    double readDouble(int id);

    /**
     * @param id in dictionary
     * @return the Integer from the dictionary by id
     */
    int readInteger(int id);

    /**
     * @param id in dictionary
     * @return the Long from the dictionary by id
     */
    long readLong(int id);

    /**
     * @param id in dictionary
     * @return the Small Int from the dictionary by id
     */
    int readSmallInt(int id);

    /**
     * @param id in dictionary
     * @return the tiny int from the dictionary by id
     */
    int readTinyInt(int id);

    /**
     * @param id in dictionary
     * @return the Boolean from the dictionary by id
     */
    boolean readBoolean(int id);

    /**
     * @param id in dictionary
     * @return the Decimal from the dictionary by id
     */
    byte[] readDecimal(int id);

    /**
     * @param id in dictionary
     * @return the TimestampData from the dictionary by id
     */
    TimestampData readTimestamp(int id);

    /**
     * @param id in dictionary
     * @return the String from the dictionary by id
     */
    byte[] readString(int id);

    /**
     * @param id in dictionary
     * @return the Varchar from the dictionary by id
     */
    byte[] readVarchar(int id);

    /**
     * @param id in dictionary
     * @return the Char from the dictionary by id
     */
    byte[] readChar(int id);
}
