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

package org.apache.flink.table.runtime.arrow;

import org.apache.flink.annotation.Internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

/** A utility class for converting byte[][] to String and String to byte[][]. */
@Internal
public class ByteArrayUtils {

    /** Convert byte[][] to String. */
    public static String twoDimByteArrayToString(byte[][] byteArray) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(byteArray);
        oos.flush();
        byte[] serializedArray = bos.toByteArray();

        return Base64.getEncoder().encodeToString(serializedArray);
    }

    /** Convert String to byte[][]. */
    public static byte[][] stringToTwoDimByteArray(String str)
            throws IOException, ClassNotFoundException {
        byte[] bytes = Base64.getDecoder().decode(str);

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bis);
        return (byte[][]) ois.readObject();
    }
}
