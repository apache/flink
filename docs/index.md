---
title: Flink Overview
---

Apache Flink (incubating) is a platform for efficient, distributed, general-purpose data processing.
It features powerful programming abstractions in Java and Scala, a high-performance runtime, and
automatic program optimization. It has native support for iterations, incremental iterations, and
programs consisting of large DAGs of operations.

If you quickly want to try out the system, please look at one of the available quickstarts. For
a thorough introduction of the Flink API please refer to the
[Programming Guide](programming_guide.html).

## Download

You can download Flink from the [downloads]({{ site.FLINK_DOWNLOAD_URL }}) page
of the [project website]({{ site.FLINK_WEBSITE_URL }}). This documentation if for version {{ site.FLINK_VERSION_STABLE }}. Be careful
when picking a version, there are different versions depending on the Hadoop and/or
HDFS version that you want to use with Flink. Please refer to [building](building.html) if you
want to build Flink yourself from the source.

In Version {{ site.FLINK_VERSION_STABLE}} the Scala API uses Scala {{ site.FLINK_SCALA_VERSION_SHORT }}. Please make
sure to use a compatible version.
