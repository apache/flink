---
title:  "Projects and Dependencies"
nav-parent_id: internals
nav-pos: 2
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

The Flink system code is divided into multiple sub-projects. The goal is to reduce the number of
dependencies that a project implementing a Flink program needs, as well as to faciltate easier testing
of smaller sub-modules.

The individual projects and their dependencies are shown in the figure below.

<div style="text-align: center;">
<img src="{{ site.baseurl }}/fig/projects_dependencies.svg" alt="The Flink sub-projects and their dependencies" height="600px" style="text-align: center;"/>
</div>

In addition to the projects listed in the figure above, Flink currently contains the following sub-projects:

- `flink-dist`: The *distribution* project. It defines how to assemble the compiled code, scripts, and other resources
into the final folder structure that is ready to use.

- `flink-quickstart`: Scripts, maven archetypes, and example programs for the quickstarts and tutorials.

- `flink-contrib`: A series of projects that are in an early version and useful tools contributed by users. The code for the latter is maintained mainly by external contributors. The requirements for code being accepted into `flink-contrib` are lower compared to the rest of the code.
