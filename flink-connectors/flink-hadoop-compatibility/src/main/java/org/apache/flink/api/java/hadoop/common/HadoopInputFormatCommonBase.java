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

package org.apache.flink.api.java.hadoop.common;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.core.io.InputSplit;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** A common base for both "mapred" and "mapreduce" Hadoop input formats. */
@Internal
public abstract class HadoopInputFormatCommonBase<T, SPITTYPE extends InputSplit>
        extends RichInputFormat<T, SPITTYPE> {
    protected transient Credentials credentials;

    protected HadoopInputFormatCommonBase(Credentials creds) {
        this.credentials = creds;
    }

    protected void write(ObjectOutputStream out) throws IOException {
        this.credentials.write(out);
    }

    public void read(ObjectInputStream in) throws IOException {
        this.credentials = new Credentials();
        credentials.readFields(in);
    }

    /**
     * @param ugi The user information
     * @return new credentials object from the user information.
     */
    public static Credentials getCredentialsFromUGI(UserGroupInformation ugi) {
        return ugi.getCredentials();
    }
}
