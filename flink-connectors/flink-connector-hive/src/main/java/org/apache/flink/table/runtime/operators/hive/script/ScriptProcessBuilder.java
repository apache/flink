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

package org.apache.flink.table.runtime.operators.hive.script;

import org.apache.flink.runtime.jobgraph.OperatorID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The builder to build {@link Process} for {@link HiveScriptTransformOperator}. The main logic is
 * from Hive's {@link org.apache.hadoop.hive.ql.exec.ScriptOperator}.
 */
public class ScriptProcessBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(ScriptProcessBuilder.class);

    private final String script;
    private final JobConf jobConf;
    private final OperatorID operatorID;

    // List of conf entries not to turn into env vars
    transient Set<String> blackListedConfEntries = null;

    public ScriptProcessBuilder(String script, JobConf hiveConf, OperatorID operatorID) {
        this.script = script;
        this.jobConf = hiveConf;
        this.operatorID = operatorID;
    }

    public Process build() throws IOException {
        String[] cmdArgs = splitArgs(script);
        String prog = cmdArgs[0];
        File currentDir = new File(".").getAbsoluteFile();

        if (!new File(prog).isAbsolute()) {
            PathFinder finder = new PathFinder("PATH");
            finder.prependPathComponent(currentDir.toString());
            // TODO: also should prepend the path contains files added
            // by user to support run user customize script after finishing supporting 'add file
            // xxx' in FLINK-27850
            File f = finder.getAbsolutePath(prog);
            if (f != null) {
                cmdArgs[0] = f.getAbsolutePath();
            }
        }
        String[] wrappedCmdArgs = addWrapper(cmdArgs);
        LOG.info("Executing " + Arrays.asList(wrappedCmdArgs));
        ProcessBuilder pb = new ProcessBuilder(wrappedCmdArgs);
        Map<String, String> env = pb.environment();
        addJobConfToEnvironment(jobConf, env);

        // Create an environment variable that uniquely identifies this script
        // operator
        String idEnvVarName = HiveConf.getVar(jobConf, HiveConf.ConfVars.HIVESCRIPTIDENVVAR);
        String idEnvVarVal = operatorID.toString();
        env.put(safeEnvVarName(idEnvVarName), idEnvVarVal);

        return pb.start();
    }

    private static class PathFinder {
        private String pathenv; // a string of pathnames
        private final String pathSep; // the path separator
        private final String fileSep; // the file separator in a directory

        /**
         * Construct a PathFinder object using the path from the specified system environment
         * variable.
         */
        public PathFinder(String envpath) {
            pathenv = System.getenv(envpath);
            pathSep = System.getProperty("path.separator");
            fileSep = System.getProperty("file.separator");
        }

        /** Appends the specified component to the path list. */
        public void prependPathComponent(String str) {
            pathenv = str + pathSep + pathenv;
        }

        /** Returns the full path name of this file if it is listed in the path. */
        public File getAbsolutePath(String filename) {
            if (pathenv == null || pathSep == null || fileSep == null) {
                return null;
            }

            int val;
            String classvalue = pathenv + pathSep;

            while (((val = classvalue.indexOf(pathSep)) >= 0) && classvalue.length() > 0) {
                //
                // Extract each entry from the pathenv
                //
                String entry = classvalue.substring(0, val).trim();
                File f = new File(entry);

                try {
                    if (f.isDirectory()) {
                        //
                        // this entry in the pathenv is a directory.
                        // see if the required file is in this directory
                        //
                        f = new File(entry + fileSep + filename);
                    }
                    //
                    // see if the filename matches and we can read it
                    //
                    if (f.isFile() && f.canRead()) {
                        return f;
                    }
                } catch (Exception ignored) {
                }
                classvalue = classvalue.substring(val + 1).trim();
            }
            return null;
        }
    }

    // Code below shameless borrowed from Hadoop Streaming
    private String[] splitArgs(String args) {
        final int outSide = 1;
        final int singLeq = 2;
        final int doubleLeq = 3;

        List<String> argList = new ArrayList<>();
        char[] ch = args.toCharArray();
        int clen = ch.length;
        int state = outSide;
        int argstart = 0;
        for (int c = 0; c <= clen; c++) {
            boolean last = (c == clen);
            int lastState = state;
            boolean endToken = false;
            if (!last) {
                if (ch[c] == '\'') {
                    if (state == outSide) {
                        state = singLeq;
                    } else if (state == singLeq) {
                        state = outSide;
                    }
                    endToken = (state != lastState);
                } else if (ch[c] == '"') {
                    if (state == outSide) {
                        state = doubleLeq;
                    } else if (state == doubleLeq) {
                        state = outSide;
                    }
                    endToken = (state != lastState);
                } else if (ch[c] == ' ') {
                    if (state == outSide) {
                        endToken = true;
                    }
                }
            }
            if (last || endToken) {
                if (c != argstart) {
                    String a;
                    a = args.substring(argstart, c);
                    argList.add(a);
                }
                argstart = c + 1;
            }
        }
        return argList.toArray(new String[0]);
    }

    /** Wrap the script in a wrapper that allows admins to control. */
    private String[] addWrapper(String[] inArgs) {
        String wrapper = HiveConf.getVar(jobConf, HiveConf.ConfVars.SCRIPTWRAPPER);
        if (wrapper == null) {
            return inArgs;
        }

        String[] wrapComponents = splitArgs(wrapper);
        int totallength = wrapComponents.length + inArgs.length;
        String[] finalArgv = new String[totallength];
        System.arraycopy(wrapComponents, 0, finalArgv, 0, wrapComponents.length);
        System.arraycopy(inArgs, 0, finalArgv, wrapComponents.length, inArgs.length);
        return finalArgv;
    }

    /**
     * addJobConfToEnvironment is mostly shamelessly copied from hadoop streaming. Added additional
     * check on environment variable length
     */
    void addJobConfToEnvironment(Configuration conf, Map<String, String> env) {
        for (Map.Entry<String, String> en : conf) {
            String name = en.getKey();
            if (!blackListed(conf, name)) {
                // String value = (String)en.getValue(); // does not apply variable
                // expansion
                String value = conf.get(name); // does variable expansion
                name = safeEnvVarName(name);
                boolean truncate =
                        conf.getBoolean(HiveConf.ConfVars.HIVESCRIPTTRUNCATEENV.toString(), false);
                value = safeEnvVarValue(value, name, truncate);
                env.put(name, value);
            }
        }
    }

    /**
     * Checks whether a given configuration name is blacklisted and should not be converted to an
     * environment variable.
     */
    private boolean blackListed(Configuration conf, String name) {
        if (blackListedConfEntries == null) {
            blackListedConfEntries = new HashSet<>();
            if (conf != null) {
                String bl =
                        conf.get(
                                HiveConf.ConfVars.HIVESCRIPT_ENV_BLACKLIST.toString(),
                                HiveConf.ConfVars.HIVESCRIPT_ENV_BLACKLIST.getDefaultValue());
                if (bl != null && !bl.isEmpty()) {
                    String[] bls = bl.split(",");
                    Collections.addAll(blackListedConfEntries, bls);
                }
            }
        }
        return blackListedConfEntries.contains(name);
    }

    private String safeEnvVarName(String name) {
        StringBuilder safe = new StringBuilder();
        int len = name.length();

        for (int i = 0; i < len; i++) {
            char c = name.charAt(i);
            char s;
            if ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
                s = c;
            } else {
                s = '_';
            }
            safe.append(s);
        }
        return safe.toString();
    }

    /**
     * Most UNIX implementations impose some limit on the total size of environment variables and
     * size of strings. To fit in this limit we need sometimes to truncate strings. Also, some
     * values tend be long and are meaningless to scripts, so strain them out.
     *
     * @param value environment variable value to check
     * @param name name of variable (used only for logging purposes)
     * @param truncate truncate value or not
     * @return original value, or truncated one if it's length is more then 20KB and truncate flag
     *     is set
     * @see <a href="http://www.kernel.org/doc/man-pages/online/pages/man2/execve.2.html">Linux Man
     *     page</a> for more details
     */
    private String safeEnvVarValue(String value, String name, boolean truncate) {
        final int lenLimit = 20 * 1024;
        if (truncate && value.length() > lenLimit) {
            value = value.substring(0, lenLimit);
            LOG.warn(
                    "Length of environment variable "
                            + name
                            + " was truncated to "
                            + lenLimit
                            + " bytes to fit system limits.");
        }
        return value;
    }
}
