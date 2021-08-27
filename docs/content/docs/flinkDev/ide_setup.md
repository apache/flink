---
title: "Importing Flink into an IDE"
weight: 4
type: docs
aliases:
  - /flinkDev/ide_setup.html
  - /internals/ide_setup.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Importing Flink into an IDE

The sections below describe how to import the Flink project into an IDE
for the development of Flink itself. For writing Flink programs, please
refer to the [Java API]({{< ref "docs/dev/datastream/project-configuration" >}})
and the [Scala API]({{< ref "docs/dev/datastream/project-configuration" >}})
quickstart guides.

{{< hint info >}}
Whenever something is not working in your IDE, try with the Maven
command line first (`mvn clean package -DskipTests`) as it might be your IDE
that has a bug or is not properly set up.
{{< /hint >}}

## Preparation

To get started, please first checkout the Flink sources from one of our
[repositories](https://flink.apache.org/community.html#source-code),
e.g.

```bash
git clone https://github.com/apache/flink.git
```

### Ignoring Refactoring Commits

We keep a list of big refactoring commits in `.git-blame-ignore-revs`. When looking at change
annotations using `git blame` it's helpful to ignore these. You can configure git and your IDE to
do so using:

```bash
git config blame.ignoreRevsFile .git-blame-ignore-revs
```

## IntelliJ IDEA

The following guide has been written for [IntelliJ IDEA](https://www.jetbrains.com/idea/download/)
2020.3. Some details might differ in other versions. Please make sure to follow all steps
accurately.

### Importing Flink

1. Choose "New" → "Project from Existing Sources".
2. Select the root folder of the cloned Flink repository.
3. Choose "Import project from external model" and select "Maven".
4. Leave the default options and successively click "Next" until you reach the SDK section.
5. If there is no SDK listed, create one using the "+" sign on the top left.
   Select "JDK", choose the JDK home directory and click "OK".
   Select the most suitable JDK version. NOTE: A good rule of thumb is to select
   the JDK version matching the active Maven profile.
6. Continue by clicking "Next" until the import is finished.
7. Open the "Maven" tab (or right-click on the imported project and find "Maven") and run
   "Generate Sources and Update Folders". Alternatively, you can run
   `mvn clean package -DskipTests`.
8. Build the Project ("Build" → "Build Project").

### Copyright Profile

Every file needs to include the Apache license as a header. This can be automated in IntelliJ by
adding a Copyright profile:

1. Go to "Settings" → "Editor" → "Copyright" → "Copyright Profiles".
2. Add a new profile and name it "Apache".
3. Add the following text as the license text:

   ```
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at
   
       http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and 
   limitations under the License.
   ```
4. Go to "Editor" → "Copyright" and choose the "Apache" profile as the default profile for this
   project.
5. Click "Apply".

### Required Plugins

Go to "Settings" → "Plugins" and select the "Marketplace" tab. Search for the following plugins,
install them, and restart the IDE if prompted:

* [Scala](https://plugins.jetbrains.com/plugin/1347-scala)
* [Python](https://plugins.jetbrains.com/plugin/631-python) – Required for PyFlink. If you do not
  intend to work on PyFlink, you can skip this.
* [Save Actions](https://plugins.jetbrains.com/plugin/7642-save-actions)
* [Checkstyle-IDEA](https://plugins.jetbrains.com/plugin/1065-checkstyle-idea)

You will also need to install the [google-java-format](https://github.com/google/google-java-format)
plugin. However, a specific version of this plugin is required. Download
[google-java-format v1.7.0.6](https://plugins.jetbrains.com/plugin/8527-google-java-format/versions/stable/115957)
and install it as follows. Make sure to never update this plugin.

1. Go to "Settings" → "Plugins".
2. Click the gear icon and select "Install Plugin from Disk".
3. Navigate to the downloaded ZIP file and select it.

#### Code Formatting

Flink uses [Spotless](https://github.com/diffplug/spotless/tree/main/plugin-maven) together with
[google-java-format](https://github.com/google/google-java-format) to format the Java code.

It is recommended to automatically format your code by applying the following settings:

1. Go to "Settings" → "Other Settings" → "google-java-format Settings".
2. Tick the checkbox to enable the plugin.
3. Change the code style to "Android Open Source Project (AOSP) style".
4. Go to "Settings" → "Other Settings" → "Save Actions".
5. Under "General", enable your preferred settings for when to format the code, e.g.
   "Activate save actions on save".
6. Under "Formatting Actions", select "Optimize imports" and "Reformat file".
7. Under "File Path Inclusions", add an entry for `.*\.java` to avoid formatting other file types.

#### Checkstyle For Java

[Checkstyle](https://checkstyle.sourceforge.io/) is used to enforce static coding guidelines.

{{< hint info >}}
Some modules are not covered by Checkstyle, e.g. flink-core, flink-optimizer, and flink-runtime.
Nevertheless, please make sure to conform to the checkstyle rules in these modules if you work in
any of these modules.
{{< /hint >}}

1. Go to "Settings" → "Tools" → "Checkstyle".
2. Set "Scan Scope" to "Only Java sources (including tests)".
3. For "Checkstyle Version" select "8.14".
4. Under "Configuration File" click the "+" icon to add a new configuration.
5. Set "Description" to "Flink".
6. Select "Use a local Checkstyle file" and point it to `tools/maven/checkstyle.xml` located within
   your cloned repository.
7. Select "Store relative to project location" and click "Next".
8. Configure the property `checkstyle.suppressions.file` with the value `suppressions.xml` and click
   "Next".
9. Click "Finish".
10. Select "Flink" as the only active configuration file and click "Apply".

You can now import the Checkstyle configuration for the Java code formatter.

1. Go to "Settings" → "Editor" → "Code Style" → "Java".
2. Click the gear icon next to "Scheme" and select "Import Scheme" → "Checkstyle Configuration".
3. Navigate to and select `tools/maven/checkstyle.xml` located within your cloned repository.

To verify the setup, click "View" → "Tool Windows" → "Checkstyle" and find the "Check Module"
button in the opened tool window. It should report no violations.

#### Checkstyle For Scala

Enable [Scalastyle](http://www.scalastyle.org/) as follows:

1. Go to "Settings" → "Editor" → "Inspections".
2. Search for "Scala style inspection" and enable it.

Now copy the file `tools/maven/scalastyle-config.xml` into the `.idea/` or `project/` folder of your
cloned repository.

#### Python for PyFlink

Working on the flink-python module requires both a Java SDK and a Python SDK. However, IntelliJ IDEA
only supports one configured SDK per module. If you intend to work actively on PyFlink, it is
recommended to import the flink-python module as a separate project either in [PyCharm](#pycharm)
or IntelliJ IDEA for working with Python.

If you only occasionally need to work on flink-python and would like to get Python to work in
IntelliJ IDEA, e.g. to run Python tests, you can use the following guide.

1. Follow [Configure a virtual environment](https://www.jetbrains.com/help/idea/creating-virtual-environment.html)
   to create a new Virtualenv Python SDK in your Flink project.
2. Find the flink-python module in the Project Explorer, right-click on it and choose
   "Open Module Settings". Alternatively, go to "Project Structure" → "Modules" and find the module
   there.
3. Change "Module SDK" to the Virtualenv Python SDK you created earlier.
4. Open the file `flink-python/setup.py` and install the dependencies when IntelliJ prompts you to
   do so.

You can verify your setup by running some of the Python tests located in flink-python.

### Common Problems

This section lists issues that developers have run into in the past when working with IntelliJ.

#### Compilation fails with `invalid flag: --add-exports=java.base/sun.net.util=ALL-UNNAMED`

This happens if the "java11" Maven profile is active, but an older JDK version is used. Go to
"View" → "Tool Windows" → "Maven" and uncheck the "java11" profile. Afterwards, reimport the
project.

#### Compilation fails with `cannot find symbol: symbol: method defineClass(...) location: class sun.misc.Unsafe`

This happens if you are using JDK 11, but are working on a Flink version which doesn't yet support
Java 11 (<= 1.9). Go to "Project Structure" → "Project Settings" → "Project" and select JDK 8 as
the Project SDK.

When switching back to newer Flink versions you may have to revert this change again.

#### Examples fail with a `NoClassDefFoundError` for Flink classes.

This happens if Flink dependencies are set to "provided", resulting in them not being available
on the classpath. You can either check "Include dependencies with 'Provided' scope" in your
run configuration, or create a test that calls the `main()` method of the example.

## Eclipse

Using Eclipse with Flink is currently not supported and discouraged. Please use
[IntelliJ IDEA](#intellij-idea) instead.

## PyCharm

If you intend to work on PyFlink, it is recommended to use
[PyCharm](https://www.jetbrains.com/pycharm/download/) as a separate IDE for the flink-python
module. The following guide has been written for 2019.1.3. Some details might differ in other
versions.

### Importing flink-python

1. Open the PyCharm IDE and choose ("File" →) "Open".
2. Select the "flink-python" folder within your located repository.

### Checkstyle For Python

[Flake8](https://pypi.org/project/flake8/) is used to enforce some coding guidelines.

1. Install flake8 for your Python interpreter using `pip install flake8`.
2. In PyCharm go to "Preferences" → "Tools" → "External Tools".
3. Select the "+" button to add a new external tool.
4. Set "Name" to "flake8".
5. Set "Description" to "Code Style Check".
6. Set "Program" to the path of your Python interpreter, e.g. `/usr/bin/python`.
7. Set "Arguments" to `-m flake8 --config=tox.ini`.
8. Set "Working Directory" to `$ProjectFileDir$`.

You can verify the setup by right-clicking on any file or folder in the flink-python project
and running "External Tools" → "flake8".

{{< top >}}
