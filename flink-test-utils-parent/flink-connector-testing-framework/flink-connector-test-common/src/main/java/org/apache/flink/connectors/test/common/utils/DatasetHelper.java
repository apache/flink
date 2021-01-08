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

package org.apache.flink.connectors.test.common.utils;

import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Random;

/** Helper functions for test data generating and validation. */
public class DatasetHelper {

    private static final int MAX_BUF_SIZE = 8192;

    public static void writeRandomBinaryToFile(File file, int size) throws Exception {
        FileOutputStream fos = new FileOutputStream(file);
        byte[] buf = new byte[MAX_BUF_SIZE];
        Random random = new Random();
        int remain = size;
        while (remain > 0) {
            random.nextBytes(buf);
            int writeLen = Math.min(remain, MAX_BUF_SIZE);
            fos.write(buf, 0, writeLen);
            remain -= writeLen;
        }
        fos.close();
    }

    public static void writeRandomTextToFile(File file, int numLine, int lengthPerLine)
            throws Exception {
        String alphaNumericString =
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "abcdefghijklmnopqrstuvwxyz" + "0123456789";
        FileWriter fw = new FileWriter(file);
        BufferedWriter bw = new BufferedWriter(fw, MAX_BUF_SIZE);
        Random random = new Random();
        for (int i = 0; i < numLine; i++) {
            for (int j = 0; j < lengthPerLine; j++) {
                bw.write(alphaNumericString.charAt(random.nextInt(alphaNumericString.length())));
            }
            bw.newLine();
        }
        bw.close();
        fw.close();
    }

    public static void appendMarkToFile(File file, String mark) throws Exception {
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);
        int character;
        char lastChar = 0;
        while ((character = br.read()) > 0) {
            lastChar = (char) character;
        }
        br.close();
        fr.close();

        FileWriter fw = new FileWriter(file, true);
        BufferedWriter bw = new BufferedWriter(fw);
        if (!(lastChar == '\n')) {
            bw.append("\n").append(mark);
        } else {
            bw.append(mark).append("\n");
        }
        bw.close();
        fw.close();
    }

    public static boolean isSame(File first, File second) throws Exception {
        return FileUtils.contentEquals(first, second);
    }
}
