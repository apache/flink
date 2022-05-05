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

package org.apache.flink.table.planner.delegation.hive.copy;

import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.HashMap;
import java.util.Map;

/** Counterpart of hive's org.apache.hadoop.hive.ql.parse.StorageFormat. */
public class HiveParserStorageFormat {
    private static final StorageFormatFactory storageFormatFactory = new StorageFormatFactory();
    private final Configuration conf;
    private String inputFormat;
    private String outputFormat;
    private String storageHandler;
    private String serde;
    private String genericName;
    private final Map<String, String> serdeProps;

    public HiveParserStorageFormat(Configuration conf) {
        this.conf = conf;
        this.serdeProps = new HashMap<>();
    }

    public void initFromStorageDescriptor(StorageDescriptor sd) {
        inputFormat = sd.getInputFormat();
        outputFormat = sd.getOutputFormat();
        serde = sd.getSerdeInfo().getSerializationLib();
        serdeProps.putAll(sd.getSerdeInfo().getParameters());
    }

    /**
     * Returns true if the passed token was a storage format token and thus was processed
     * accordingly.
     */
    public boolean fillStorageFormat(HiveParserASTNode child) throws SemanticException {
        switch (child.getToken().getType()) {
            case HiveASTParser.TOK_TABLEFILEFORMAT:
                if (child.getChildCount() < 2) {
                    throw new SemanticException(
                            "Incomplete specification of File Format. "
                                    + "You must provide InputFormat, OutputFormat.");
                }
                inputFormat =
                        HiveParserBaseSemanticAnalyzer.unescapeSQLString(
                                child.getChild(0).getText());
                outputFormat =
                        HiveParserBaseSemanticAnalyzer.unescapeSQLString(
                                child.getChild(1).getText());
                if (child.getChildCount() == 3) {
                    serde =
                            HiveParserBaseSemanticAnalyzer.unescapeSQLString(
                                    child.getChild(2).getText());
                }
                break;
            case HiveASTParser.TOK_STORAGEHANDLER:
                storageHandler =
                        HiveParserBaseSemanticAnalyzer.unescapeSQLString(
                                child.getChild(0).getText());
                if (child.getChildCount() == 2) {
                    HiveParserBaseSemanticAnalyzer.readProps(
                            (HiveParserASTNode) (child.getChild(1).getChild(0)), serdeProps);
                }
                break;
            case HiveASTParser.TOK_FILEFORMAT_GENERIC:
                HiveParserASTNode grandChild = (HiveParserASTNode) child.getChild(0);
                genericName = (grandChild == null ? "" : grandChild.getText()).trim().toUpperCase();
                processStorageFormat(genericName);
                break;
            default:
                // token was not a storage format token
                return false;
        }
        return true;
    }

    protected void processStorageFormat(String name) throws SemanticException {
        if (name.isEmpty()) {
            throw new SemanticException("File format in STORED AS clause cannot be empty");
        }
        StorageFormatDescriptor descriptor = storageFormatFactory.get(name);
        if (descriptor == null) {
            throw new SemanticException(
                    "Unrecognized file format in STORED AS clause:" + " '" + name + "'");
        }
        inputFormat = descriptor.getInputFormat();
        outputFormat = descriptor.getOutputFormat();
        if (serde == null) {
            serde = descriptor.getSerde();
        }
        if (serde == null) {
            // RCFile supports a configurable SerDe
            if (name.equalsIgnoreCase(IOConstants.RCFILE)) {
                serde = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEDEFAULTRCFILESERDE);
            } else {
                serde = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEDEFAULTSERDE);
            }
        }
    }

    public void fillDefaultStorageFormat(boolean isExternal, boolean isMaterializedView)
            throws SemanticException {
        if ((inputFormat == null) && (storageHandler == null)) {
            String defaultFormat;
            String defaultManagedFormat;
            if (isMaterializedView) {
                defaultFormat =
                        defaultManagedFormat = conf.get("hive.materializedview.fileformat", "ORC");
                serde =
                        conf.get(
                                "hive.materializedview.serde",
                                "org.apache.hadoop.hive.ql.io.orc.OrcSerde");
            } else {
                defaultFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT);
                defaultManagedFormat = conf.get("hive.default.fileformat.managed", "none");
            }

            if (!isExternal && !"none".equals(defaultManagedFormat)) {
                defaultFormat = defaultManagedFormat;
            }

            if (StringUtils.isNullOrWhitespaceOnly(defaultFormat)) {
                inputFormat = IOConstants.TEXTFILE_INPUT;
                outputFormat = IOConstants.TEXTFILE_OUTPUT;
            } else {
                processStorageFormat(defaultFormat);
                if (defaultFormat.equalsIgnoreCase(IOConstants.RCFILE)) {
                    serde = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEDEFAULTRCFILESERDE);
                }
            }
        }
    }

    public void setSerde(String serde) {
        this.serde = serde;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public String getStorageHandler() {
        return storageHandler;
    }

    public String getSerde() {
        return serde;
    }

    public Map<String, String> getSerdeProps() {
        return serdeProps;
    }

    public String getGenericName() {
        return genericName;
    }
}
