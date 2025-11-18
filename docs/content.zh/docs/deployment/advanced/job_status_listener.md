
---
title: "作业状态改变监听器"
nav-title: job-status-listener
nav-parent_id: advanced
nav-pos: 5
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

## 作业状态改变监听器
Flink 为用户提供了一个可插入接口，用于注册处理作业状态变化的自定义逻辑，其中提供了有关源/接收器的沿袭信息。这使用户能够实现自己的 Flink 数据血缘报告器，将沿袭信息发送到第三方数据沿袭系统，例如 Datahub 和 Openlineage。

每次应用程序发生状态更改时，都会触发作业状态更改监听器。数据沿袭信息包含在 JobCreatedEvent 中。

### 为你的自定义丰富器实现插件

要实现自定义 JobStatusChangedListener 插件，您需要：

- 添加自己的 JobStatusChangedListener 通过实现 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/core/execution/JobStatusChangedListener.java" name="JobStatusChangedListener" >}} 接口。

- 添加自己的 JobStatusChangedListenerFactory 通过实现 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/core/execution/JobStatusChangedListenerFactory.java" name="JobStatusChangedListenerFactory" >}} 接口。

- 添加Java服务条目。创建文件 `META-INF/services/org.apache.flink.core.execution.JobStatusChangedListenerFactory` 其中包含您的作业状态更改侦听器工厂类的类名 (请看 [Java Service Loader](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/ServiceLoader.html) 文档了解更多详情)。


然后，创建一个包含 `JobStatusChangedListener`, `JobStatusChangedListenerFactory`, `META-INF/services/` 以及所有外部依赖项的 Java 库.
在 Flink 发行版的 `plugins/` 中创建一个目录，使用任意名称，例如“job-status-changed-listener”，并将 jar 放入此目录中。
有关更多详细信息，请参阅 [Flink Plugin]({{< ref "docs/deployment/filesystems/plugins" >}})。

JobStatusChangedListenerFactory 示例:

``` java
package org.apache.flink.test.execution;

public static class TestingJobStatusChangedListenerFactory
        implements JobStatusChangedListenerFactory {

    @Override
    public JobStatusChangedListener createListener(Context context) {
        return new TestingJobStatusChangedListener();
    }
}
```

JobStatusChangedListener 示例:

``` java
package org.apache.flink.test.execution;

private static class TestingJobStatusChangedListener implements JobStatusChangedListener {

    @Override
    public void onEvent(JobStatusChangedEvent event) {
        statusChangedEvents.add(event);
    }
}
```

### 配置

Flink 组件在启动时加载 JobStatusChangedListener 插件。为确保加载 JobStatusChangedListener 的所有实现，所有类名都应定义在 [execution.job-status-changed-listeners]({{< ref "docs/deployment/config#execution.job-status-changed-listeners" >}}).
如果此配置为空，则不会启动任何监听器。例如
```
    execution.job-status-changed-listeners = org.apache.flink.test.execution.TestingJobStatusChangedListenerFactory
```

{{< top >}}
