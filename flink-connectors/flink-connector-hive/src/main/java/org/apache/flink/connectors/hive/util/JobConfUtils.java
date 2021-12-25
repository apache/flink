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

package org.apache.flink.connectors.hive.util;

import org.apache.flink.api.java.hadoop.common.HadoopInputFormatCommonBase;
import org.apache.flink.connectors.hive.JobConfWrapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/** Utilities for {@link JobConf}. */
public class JobConfUtils {

    /**
     * Gets the {@link HiveConf.ConfVars#DEFAULTPARTITIONNAME} value from the {@link
     * JobConfWrapper}.
     */
    public static String getDefaultPartitionName(JobConfWrapper confWrapper) {
        return getDefaultPartitionName(confWrapper.conf());
    }

    /** Gets the {@link HiveConf.ConfVars#DEFAULTPARTITIONNAME} value from the {@link JobConf}. */
    public static String getDefaultPartitionName(JobConf jobConf) {
        return jobConf.get(
                HiveConf.ConfVars.DEFAULTPARTITIONNAME.varname,
                HiveConf.ConfVars.DEFAULTPARTITIONNAME.defaultStrVal);
    }

    private static void addCredentialsIntoJobConf(JobConf jobConf) {
        UserGroupInformation currentUser = null;
        try {
            currentUser = UserGroupInformation.getCurrentUser();
        } catch (IOException e) {
            throw new RuntimeException("Unable to determine current user", e);
        }
        Credentials currentUserCreds =
                HadoopInputFormatCommonBase.getCredentialsFromUGI(currentUser);
        if (currentUserCreds != null) {
            jobConf.getCredentials().mergeAll(currentUserCreds);
        }
    }

    public static JobConf createJobConfWithCredentials(Configuration configuration) {
        JobConf jobConf = new JobConf(configuration);
        addCredentialsIntoJobConf(jobConf);
        return jobConf;
    }
}
