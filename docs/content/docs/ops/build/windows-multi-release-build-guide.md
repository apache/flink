---
title: "Building Multiple Flink Releases on Windows"
weight: 120
type: docs
aliases:
  - /docs/dev/build/windows-multi-release
---

<!-- Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License. -->

# Building Multiple Flink Releases on Windows

This guide describes how to build multiple historical releases of Apache Flink on Windows using Maven.  
Although most users build only the release that matches their application, certain research, academic, and analysis scenarios may require building many versions—e.g., when using metric extraction and static-analysis tools such as SpotBugs.

This document provides practical, verified build instructions for the following releases:

- 2.1.0  
- 2.0.0  
- 1.20.0  
- 1.19.0  
- 1.18.0  
- 1.17.0  
- 1.16.0  
- 1.15.0  
- 1.14.0  
- 1.13.0  
- 1.12.0  
- 1.11.0  
- 1.10.0  
- 1.9.0  
- 1.8.0  
- 1.7.0  
- 1.6.0  
- 1.5.0  
- 1.4.0  
- 1.3.0  

The build process is documented from the newest release down to the oldest, as older versions require increasingly specific workarounds.

---

## Prerequisites

The instructions below were last verified on:

- **Windows 11 Home Single Language (version 25H2)**  
- **PowerShell**  
- **Maven 3.8.6**  
- **64-bit Java runtimes**

To avoid issues with long file paths and Unicode characters, place all release directories under `C:\releases`, for example:

C:\ <br>
|_releases<br>
| |\_\_\_\_flink-release-x.x.x<br>
| |\_\_\_\_flink-release-x.x.x<br>
| |\_\_\_\_etc...<br>
|etc...<br>


Some Flink modules (e.g., `flink-table-planner_2.11`) generate paths close to Windows’ 260-character limit. Keeping paths short avoids build failures.

---

# Build Instructions by Release

Each release section below includes the required Java version, recommended Maven commands, and workarounds for shaded JAR generation, blocked HTTP downloads, or missing legacy dependencies.

---

## Flink 2.1.0, 2.0.0, 1.20.0, 1.19.0, 1.18.0

**Java:** 17.0.12  
**Maven:** 3.8.6  

Run:

```powershell
mvn spotless:apply
mvn clean install -DskipTests -Pskip-webui-build -T 1C
```
**Notes:**
<ul>
<li>spotless:apply formats the source code according to formatting rules defined in the POM.</li>
<li>-T 1C flag uses one thread per CPU core to speed up builds.</li>
</ul>

---

## Flink 1.17.0

**Java:** 11.0.28  
**Maven:** 3.8.6 

Starting with Flink 1.17 and below, multi-threaded builds may hang during the creation of shaded JARs. **Do not use** ```-T``` **for these versions**

Run:

```powershell
mvn spotless:apply
mvn clean install -DskipTests -Pskip-webui-build
```

---

## Flink 1.16.0, 1.15.0, 1.14.0, 1.13.0

**Java:** 11.0.28  
**Maven:** 3.8.6

These versions may attempt to download the dependency
`pentaho-aggdesigner-algorithm-5.1.5-jhyde`
via HTTP, which Maven 3.8.x blocks by default.

Because the original repository is no longer available, download the dependency manually:

### Workaround for pentaho-aggdesigner-algorithm

<ol>
    <li>Go to: 
        https://repo.huaweicloud.com/repository/maven/huaweicloudsdk/org/pentaho/pentaho-aggdesigner-algorithm/5.1.5-jhyde/
    </li>
    <li>
        Download:
        <ul>
            <li>
                pentaho-aggdesigner-algorithm-5.1.5-jhyde.jar
            </li>
            <li> 
                pentaho-aggdesigner-algorithm-5.1.5-jhyde.pom
            </li>
        </ul>
    </li>
    <li>
        Place both files in your local Maven repository:

        C:\Users\<user>\.m2\repository

<li>Remove the script tag from the POM file.

<li>Install manually:
</ol>

```powershell
mvn install:install-file
  -Dfile="path\to\pentaho-aggdesigner-algorithm-5.1.5-jhyde.jar" 
  -DpomFile="path\to\pentaho-aggdesigner-algorithm-5.1.5-jhyde.pom"
```
---

## Flink 1.12.0, 1.11.0

**Java:** 11.0.28  
**Maven:** 3.8.6

Use:

```powershell
mvn clean install -DskipTests -Pskip-webui-build
```

Apply the shaded-JAR and Pentaho workarounds from previous sections if needed.

---

## Flink 1.10.0

**Java:** 11.0.28  
**Maven:** 3.8.6

This version may attempt to download:<br>
```jms-1.1.jar```<br>
via HTTP, which Maven 3.8.x blocks.

### Workaround for JMS 1.1

Download:
    <ul>
        <li>https://repository.jboss.org/maven2/javax/jms/jms/1.1/jms-1.1.jar</li>
        <li>https://repository.jboss.org/maven2/javax/jms/jms/1.1/jms-1.1.pom</li>
    </ul>

Place them in:

```C:\Users\<user>\.m2\repository```


Then remove the script tag from the POM and install manually:

```powershell mvn install:install-file
  -Dfile="path\to\jms-1.1.jar" 
  -DpomFile="path\to\jms-1.1.pom"
```


Build with:

```powershell 
mvn clean install -DskipTests -Pskip-webui-build
```

## Flink 1.9.0

**Java:** 1.8.0_202  
**Maven:** 3.8.6

Older releases may require additional memory during compilation.
Allocate more heap space before building:

```
$env:MAVEN_OPTS="-Xmx2g -Xms1g -XX:MaxPermSize=512m -XX:+UseG1GC"
```


Then:

```
mvn clean install -DskipTests -Pskip-webui-build
```

## Flink 1.8.0, 1.7.0, 1.6.0, 1.5.0, 1.4.0, 1.3.0

**Java:** 1.8.0_202  
**Maven:** 3.8.6

These versions may require all workarounds listed earlier:

<ul>
    <li>shaded-JAR hangs</li>
    <li>Pentaho dependency</li>
    <li>JMS dependency</li>
    <li>additional heap memory</li>
</ul>

Additionally, versions ≤ 1.8.0 may try to download:

**maprfs-5.2.1-mapr (HTTP-only)**

This dependency is no longer available and has many transitive dependencies.
Skip this module instead of attempting manual installation.

**confluent.io dependencies**

These can still be downloaded after disabling Maven mirrors.

Build using:

```powershell
mvn clean install -DskipTests -pl "!flink-filesystems/flink-mapr-fs"
```

Flags:

<ul>
<li> -DskipTests — compiles but skips execution of tests</li>

<li>-Pskip-webui-build — skips the Web UI build</li>

<li>-pl "!flink-filesystems/flink-mapr-fs" — excludes MapR FS from the build</li>
</ul>

## Summary

Building historical Flink releases on Windows may require:

<ul>
<li>selecting the correct Java version</li>

<li>avoiding multi-threaded Maven builds for older versions</li>

<li>manually installing legacy dependencies that no longer exist online</li>

<li>allocating additional heap memory</li>

<li>skipping the MapR filesystem module</li>

<li>These instructions have been validated across all releases from 1.3.0 to 2.1.0.</li>
</ul>

If you encounter issues not covered here, consider checking your local Maven cache, ensuring path length limits are not exceeded, or reviewing the release-specific build notes available in the source distribution.