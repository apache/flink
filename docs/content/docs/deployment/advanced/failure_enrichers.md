---
title: "Failure Enrichers"
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

## Custom failure enrichers
Flink provides a pluggable interface for users to register their custom logic and enrich failures with extra metadata labels (string key-value pairs).
This enables users to implement their own failure enrichment plugins to categorize job failures, expose custom metrics, or make calls to external notification systems.

FailureEnrichers are triggered every time an exception is reported at runtime by the JobManager.
Every FailureEnricher may asynchronously return labels associated with the failure that are then exposed via the JobManager's REST API (e.g., a 'type:System' label implying the failure is categorized as a system error).


### Implement a plugin for your custom enricher

To implement a custom FailureEnricher plugin, you need to:

- Add your own FailureEnricher by implementing the {{< gh_link file="/flink-core/src/main/java/org/apache/flink/core/failure/FailureEnricher.java" name="FailureEnricher" >}} interface.

- Add your own FailureEnricherFactory by implementing the {{< gh_link file="/flink-core/src/main/java/org/apache/flink/core/failure/FailureEnricherFactory.java" name="FailureEnricherFactory" >}} interface.

- Add a service entry. Create a file `META-INF/services/org.apache.flink.core.failure.FailureEnricherFactory` which contains the class name of your failure enricher factory class (see [Java Service Loader](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/ServiceLoader.html) docs for more details).


Then, create a jar which includes your `FailureEnricher`, `FailureEnricherFactory`, `META-INF/services/` and all external dependencies.
Make a directory in `plugins/` of your Flink distribution with an arbitrary name, e.g. "failure-enrichment", and put the jar into this directory.
See [Flink Plugin]({{< ref "docs/deployment/filesystems/plugins" >}}) for more details.

{{< hint warning >}}
Note that every FailureEnricher should have defined a set of {{< gh_link file="/flink-core/src/main/java/org/apache/flink/core/failure/FailureEnricher.java" name="output keys" >}} that may be associated with values. This set of keys has to be unique otherwise all enrichers with overlapping keys will be ignored.
{{< /hint >}}

FailureEnricherFactory example:

``` java
package org.apache.flink.test.plugin.jar.failure;

public class TestFailureEnricherFactory implements FailureEnricherFactory {

   @Override
   public FailureEnricher createFailureEnricher(Configuration conf) {
        return new CustomEnricher();
   }
}
```

FailureEnricher example:

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

### Configuration

The JobManager loads FailureEnricher plugins at startup. To make sure your FailureEnrichers are loaded all class names should be defined as part of [jobmanager.failure-enrichers configuration]({{< ref "docs/deployment/config#jobmanager-failure-enrichers" >}}).
  If this configuration is empty, NO enrichers will be started. Example:
```
    jobmanager.failure-enrichers = org.apache.flink.test.plugin.jar.failure.CustomEnricher
```

### Validation

To validate that your FailureEnricher is loaded, you can check the JobManager logs for the following line:
```
    Found failure enricher org.apache.flink.test.plugin.jar.failure.CustomEnricher at jar:file:/path/to/flink/plugins/failure-enrichment/flink-test-plugin.jar!/org/apache/flink/test/plugin/jar/failure/CustomEnricher.class
```

Moreover, you can query the JobManager's [REST API]({{< ref "docs/ops/rest_api" >}}#jobs-jobid-exceptions) looking for the failureLabels field:
```
    "failureLabels": {
        "labelKey": "labelValue"
    }
```
