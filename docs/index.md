---
title: Flink Overview
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

Apache Flink is a platform for efficient, distributed, general-purpose data processing.
It features powerful programming abstractions in Java and Scala, a high-performance runtime, and
automatic program optimization. It has native support for iterations, incremental iterations, and
programs consisting of large DAGs of operations.

If you quickly want to try out the system, please look at one of the available quickstarts. For
a thorough introduction of the Flink API please refer to the
[Programming Guide](programming_guide.html).

## Download

You can download Flink from the [downloads]({{ site.FLINK_DOWNLOAD_URL }}) page
of the [project website]({{ site.FLINK_WEBSITE_URL }}). This documentation is for version {{ site.FLINK_VERSION_STABLE }}. Be careful
when picking a version, there are different versions depending on the Hadoop and/or
HDFS version that you want to use with Flink. Please refer to [building](building.html) if you
want to build Flink yourself from the source.

In Version {{ site.FLINK_VERSION_STABLE}} the Scala API uses Scala {{ site.FLINK_SCALA_VERSION_SHORT }}. Please make
sure to use a compatible version.
