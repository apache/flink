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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;

import java.util.ArrayList;
import java.util.List;

/** Tests that the {@link JarListInfo} can be marshalled and unmarshalled. */
public class JarListInfoTest extends RestResponseMarshallingTestBase<JarListInfo> {
    @Override
    protected Class<JarListInfo> getTestResponseClass() {
        return JarListInfo.class;
    }

    @Override
    protected JarListInfo getTestResponseInstance() throws Exception {
        List<JarListInfo.JarEntryInfo> jarEntryList1 = new ArrayList<>();
        jarEntryList1.add(new JarListInfo.JarEntryInfo("name1", "desc1"));
        jarEntryList1.add(new JarListInfo.JarEntryInfo("name2", "desc2"));

        List<JarListInfo.JarEntryInfo> jarEntryList2 = new ArrayList<>();
        jarEntryList2.add(new JarListInfo.JarEntryInfo("name3", "desc3"));
        jarEntryList2.add(new JarListInfo.JarEntryInfo("name4", "desc4"));

        List<JarListInfo.JarFileInfo> jarFileList = new ArrayList<>();
        jarFileList.add(
                new JarListInfo.JarFileInfo(
                        "fileId1", "fileName1", System.currentTimeMillis(), jarEntryList1));
        jarFileList.add(
                new JarListInfo.JarFileInfo(
                        "fileId2", "fileName2", System.currentTimeMillis(), jarEntryList2));

        return new JarListInfo("local", jarFileList);
    }
}
