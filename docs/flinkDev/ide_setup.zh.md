---
title: "导入 Flink 到 IDE 中"
nav-parent_id: flinkdev
nav-pos: 3
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

* Replaced by the TOC
{:toc}

The sections below describe how to import the Flink project into an IDE
for the development of Flink itself. For writing Flink programs, please
refer to the [Java API]({{ site.baseurl }}/dev/project-configuration.html)
and the [Scala API]({{ site.baseurl }}/dev/project-configuration)
quickstart guides.

**NOTE:** Whenever something is not working in your IDE, try with the Maven
command line first (`mvn clean package -DskipTests`) as it might be your IDE
that has a bug or is not properly set up.

## Preparation

To get started, please first checkout the Flink sources from one of our
[repositories](https://flink.apache.org/community.html#source-code),
e.g.
{% highlight bash %}
git clone https://github.com/apache/flink.git
{% endhighlight %}

## IntelliJ IDEA

A brief guide on how to set up IntelliJ IDEA IDE for development of the Flink core.
As Eclipse is known to have issues with mixed Scala and Java projects, more and more contributors are migrating to IntelliJ IDEA.

The following documentation describes the steps to setup IntelliJ IDEA 2019.1.3
([https://www.jetbrains.com/idea/download/](https://www.jetbrains.com/idea/download/))
with the Flink sources.

### Installing the Scala plugin

The IntelliJ installation setup offers to install the Scala plugin.
If it is not installed, follow these instructions before importing Flink
to enable support for Scala projects and files:

1. Go to IntelliJ plugins settings (IntelliJ IDEA -> Preferences -> Plugins) and
   click on "Install Jetbrains plugin...".
2. Select and install the "Scala" plugin.
3. Restart IntelliJ

### Importing Flink

1. Start IntelliJ IDEA and choose "New -> Project from Existing Sources"
2. Select the root folder of the cloned Flink repository
3. Choose "Import project from external model" and select "Maven"
4. Leave the default options and successively click "Next" until you reach the SDK section.
5. If there is no SDK listed, create one using the "+" sign on the top left.
   Select "JDK", choose the JDK home directory and click "OK".
   Select the most suiting JDK version. NOTE: A good rule of thumb is to select
   the JDK version matching the active Maven profile.
6. Continue by clicking "Next" until finishing the import.
7. Right-click on the imported Flink project -> Maven -> Generate Sources and Update Folders.
   Note that this will install Flink libraries in your local Maven repository,
   located by default at "/home/$USER/.m2/repository/org/apache/flink/".
   Alternatively, `mvn clean package -DskipTests` also creates the files necessary
   for the IDE to work but without installing the libraries.
8. Build the Project (Build -> Make Project)

### Checkstyle For Java
IntelliJ supports checkstyle within the IDE using the Checkstyle-IDEA plugin.

1. Install the "Checkstyle-IDEA" plugin from the IntelliJ plugin repository.
2. Configure the plugin by going to Settings -> Other Settings -> Checkstyle.
3. Set the "Scan Scope" to "Only Java sources (including tests)".
4. Select _8.14_ in the "Checkstyle Version" dropdown and click apply. **This step is important,
   don't skip it!**
5. In the "Configuration File" pane, add a new configuration using the plus icon:
    1. Set the "Description" to "Flink".
    2. Select "Use a local Checkstyle file", and point it to
      `"tools/maven/checkstyle.xml"` within
      your repository.
    3. Check the box for "Store relative to project location", and click
      "Next".
    4. Configure the "checkstyle.suppressions.file" property value to
      `"suppressions.xml"`, and click "Next", then "Finish".
6. Select "Flink" as the only active configuration file, and click "Apply" and
   "OK".
7. Checkstyle will now give warnings in the editor for any Checkstyle
   violations.

Once the plugin is installed you can directly import `"tools/maven/checkstyle.xml"` by going to Settings -> Editor -> Code Style -> Java -> Gear Icon next to Scheme dropbox. This will for example automatically adjust the imports layout.

You can scan an entire module by opening the Checkstyle tools window and
clicking the "Check Module" button. The scan should report no errors.

<span class="label label-info">Note</span> Some modules are not fully covered by checkstyle,
which include `flink-core`, `flink-optimizer`, and `flink-runtime`.
Nevertheless please make sure that code you add/modify in these modules still conforms to the checkstyle rules.

### Checkstyle For Scala

Enable scalastyle in Intellij by selecting Settings -> Editor -> Inspections, then searching for "Scala style inspections". Also Place `"tools/maven/scalastyle-config.xml"` in the `"<root>/.idea"` or `"<root>/project"` directory.

### FAQ

This section lists issues that developers have run into in the past when working with IntelliJ:

- Compilation fails with `invalid flag: --add-exports=java.base/sun.net.util=ALL-UNNAMED`

This means that IntelliJ activated the `java11` profile despite an older JDK being used.
Open the Maven tool window (View -> Tool Windows -> Maven), uncheck the `java11` profile and reimport the project.

- Compilation fails with `cannot find symbol: symbol: method defineClass(...) location: class sun.misc.Unsafe`

This means that IntelliJ is using JDK 11 for the project, but you are working on a Flink version which doesn't
support Java 11.
This commonly happens when you have setup IntelliJ to use JDK 11 and checkout older versions of Flink (<= 1.9).
Open the project settings window (File -> Project Structure -> Project Settings: Project) and select JDK 8 as the project
SDK.
You may have to revert this after switching back to the new Flink version if you want to use JDK 11.

- Examples fail with a `NoClassDefFoundError` for Flink classes.

This is likely due to Flink dependencies being set to provided, resulting in them not being put automatically on the 
classpath.
You can either tick the "Include dependencies with 'Provided' scope" box in the run configuration, or create a test
that calls the `main()` method of the example (`provided` dependencies are available on the test classpath).

## Eclipse

**NOTE:** From our experience, this setup does not work with Flink
due to deficiencies of the old Eclipse version bundled with Scala IDE 3.0.3 or
due to version incompatibilities with the bundled Scala version in Scala IDE 4.4.1.

**We recommend to use IntelliJ instead (see [above](#intellij-idea))**

## PyCharm

A brief guide on how to set up PyCharm for development of the flink-python module.

The following documentation describes the steps to setup PyCharm 2019.1.3
([https://www.jetbrains.com/pycharm/download/](https://www.jetbrains.com/pycharm/download/))
with the Flink Python sources.

### Importing flink-python
If you are in the PyCharm startup interface:

1. Start PyCharm and choose "Open"
2. Select the flink-python folder in the cloned Flink repository

If you have used PyCharm to open a project:

1. Select "File -> Open"
2. Select the flink-python folder in the cloned Flink repository


### Checkstyle For Python
The Python code checkstyle of Apache Flink should create a flake8 external tool in the project. 

1. Install the flake8 in the used Python interpreter(refer to ([https://pypi.org/project/flake8/](https://pypi.org/project/flake8/)).
2. Select "PyCharm -> Preferences... -> Tools -> External Tools -> + (The bottom left corner of the page on the right)", next configure the external tool.
3. Set the "Name" to "flake8".
4. Set the "Description" to "code style check".
5. Set the "Program"  to the Python interpreter path(e.g. /usr/bin/python).
6. Set the "Arguments" to "-m flake8 \-\-config=tox.ini"
7. Set the "Working directory" to "$ProjectFileDir$"

Now, you can check your Python code style by the operation:
    "Right click in any file or folder in the flink-python project -> External Tools -> flake8"
    
{% top %}
