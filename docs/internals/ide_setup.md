---
title: "IDE Setup"
nav-parent_id: start
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
refer to the [Java API]({{ site.baseurl }}/quickstart/java_api_quickstart.html)
and the [Scala API]({{ site.baseurl }}/quickstart/scala_api_quickstart.html)
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

The following documentation describes the steps to setup IntelliJ IDEA 2016.2.5
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

1. Start IntelliJ IDEA and choose "Import Project"
2. Select the root folder of the Flink repository
3. Choose "Import project from external model" and select "Maven"
4. Leave the default options and click on "Next" until you hit the SDK section.
5. If there is no SDK, create a one with the "+" sign top left,
   then click "JDK", select your JDK home directory and click "OK".
   Otherwise simply select your SDK.
6. Continue by clicking "Next" again and finish the import.
7. Right-click on the imported Flink project -> Maven -> Generate Sources and Update Folders.
   Note that this will install Flink libraries in your local Maven repository,
   i.e. "/home/*-your-user-*/.m2/repository/org/apache/flink/".
   Alternatively, `mvn clean package -DskipTests` also creates the necessary
   files for the IDE to work with but without installing libraries.
8. Build the Project (Build -> Make Project)

### Checkstyle
IntelliJ supports checkstyle within the IDE using the Checkstyle-IDEA plugin.

1. Install the "Checkstyle-IDEA" plugin from the IntelliJ plugin repository.
2. Configure the plugin by going to Settings -> Other Settings -> Checkstyle.
3. Set the "Scan Scope" to "Only Java sources (including tests)".
4. Select _8.4_ in the "Checkstyle Version" dropdown and click apply. **This step is important,
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

## Eclipse

**NOTE:** From our experience, this setup does not work with Flink
due to deficiencies of the old Eclipse version bundled with Scala IDE 3.0.3 or
due to version incompatibilities with the bundled Scala version in Scala IDE 4.4.1.

**We recommend to use IntelliJ instead (see [above](#intellij-idea))**

{% top %}
