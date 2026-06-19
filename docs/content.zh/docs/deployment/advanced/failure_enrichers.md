---
title: "故障丰富器"
nav-title: failure-enrichers
nav-parent_id: advanced
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

## 自定义故障丰富器（Failure Enricher）

Flink 提供了一个可插拔的接口，供用户注册他们自定义的逻辑，并使用额外的元数据标签（字符串类型的 key-value 对）来丰富故障（Failure）信息。
这使得用户可以实现自定义的故障丰富器插件，对作业故障进行分类、对外暴露自定义指标，或者调用外部通知系统。

每当 JobManager 在运行时收到异常时，都会触发 FailureEnrichers。
每个 FailureEnricher 可以异步返回故障对应的标签（labels），这些标签可以通过 JobManager 的 REST API 来查询（例如："type:System" 的标签意味着该故障被分类为系统错误）。

### 实现一个自定义的故障丰富器插件

要实现自定义的 FailureEnricher 插件，需要按照下面的步骤：

- 通过实现 FailureEnricher 接口，添加自定义的 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/core/failure/FailureEnricher.java" name="FailureEnricher" >}}；

- 通过实现 FailureEnricherFactory 接口，添加自定义的 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/core/failure/FailureEnricherFactory.java" name="FailureEnricherFactory" >}}；

- 添加一个 Service 入口，创建一个文件 `META-INF/services/org.apache.flink.core.failure.FailureEnricherFactory`，文件中包含自定义 FailureEnricherFactory 的类名（有关详细信息，请参阅 [Java Service Loader](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/ServiceLoader.html) 文档）。

然后，创建一个包含 `FailureEnricher`，`FailureEnricherFactory`，`META-INF/services/` 和所有外部依赖项的 jar 文件。
在 Flink Deploy 目录中的 `plugins/` 目录中创建一个任意名称的目录，例如 "failure-enrichment"，并将 jar 文件放入此目录中。
使用方式与其它 Plugin 是类似的，有关的详细信息，请参考 [Flink Plugins]({{< ref "docs/deployment/filesystems/plugins" >}})。

{{< hint warning >}}
请注意，每个 FailureEnricher 都应该定义一组与对应 values 关联的 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/core/failure/FailureEnricher.java" name="output keys" >}}。这组 keys 必须是唯一的，否则将忽略具有重复 key 的所有丰富器（enrichers）。
{{< /hint >}}

FailureEnricherFactory 示例:

``` java
package org.apache.flink.test.plugin.jar.failure;

public class TestFailureEnricherFactory implements FailureEnricherFactory {

   @Override
   public FailureEnricher createFailureEnricher(Configuration conf) {
        return new CustomEnricher();
   }
}
```

FailureEnricher 示例:

``` java
package org.apache.flink.test.plugin.jar.failure;

public class CustomEnricher implements FailureEnricher {
    private final Set<String> outputKeys;
    
    public CustomEnricher() {
        this.outputKeys = Collections.singleton("labelKey");
    }

    @Override
    public Set<String> getOutputKeys() {
        return outputKeys;
    }

    @Override
    public CompletableFuture<Map<String, String>> processFailure(
            Throwable cause, Context context) {
        return CompletableFuture.completedFuture(Collections.singletonMap("labelKey", "labelValue"));
    }
}
```

### 配置

JobManager 在启动时加载 FailureEnricher 插件。 为了确保您的 FailureEnrichers 已加载，所有类名都应该定义在 [jobmanager.failure-enrichers 配置]({{< ref "docs/deployment/config#jobmanager-failure-enrichers" >}}) 中。
如果此配置为空，则不会启动任何 Enricher。 示例：

```
    jobmanager.failure-enrichers = org.apache.flink.test.plugin.jar.failure.CustomEnricher
```

### 验证

要验证 FailureEnricher 是否已加载，您可以检查 JobManager 日志中是否有下面的内容：

```
    Found failure enricher org.apache.flink.test.plugin.jar.failure.CustomEnricher at jar:file:/path/to/flink/plugins/failure-enrichment/flink-test-plugin.jar!/org/apache/flink/test/plugin/jar/failure/CustomEnricher.class
```

此外，还可以查询 JobManager 的 [REST API]({{< ref "docs/ops/rest_api" >}}#jobs-jobid-exceptions) 来查找 failureLabels 字段：

```
    "failureLabels": {
        "labelKey": "labelValue"
    }
```
