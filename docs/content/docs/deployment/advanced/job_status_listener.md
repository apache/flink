
---
title: "Job Status Changed Listener"
nav-title: job-status-listener
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

## Job status changed listener
Flink provides a pluggable interface for users to register their custom logic for handling with the job status changes in which lineage info about source/sink is provided.
This enables users to implement their own flink lineage reporter to send lineage info to third party data lineage systems for example Datahub and Openlineage.

The job status changed listeners are triggered every time status change happened for the application. The data lineage info is included in the JobCreatedEvent.

### Implement a plugin for Job status changed listener

To implement a custom JobStatusChangedListener plugin, you need to:

- Add your own JobStatusChangedListener by implementing the {{< gh_link file="/flink-core/src/main/java/org/apache/flink/core/execution/JobStatusChangedListener.java" name="JobStatusChangedListener" >}} interface.

- Add your own JobStatusChangedListenerFactory by implementing the {{< gh_link file="/flink-core/src/main/java/org/apache/flink/core/execution/JobStatusChangedListenerFactory.java" name="JobStatusChangedListenerFactory" >}} interface.

- Add a service entry. Create a file `META-INF/services/org.apache.flink.core.execution.JobStatusChangedListenerFactory` which contains the class name of your job status changed listener factory class (see [Java Service Loader](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/ServiceLoader.html) docs for more details).


Then, create a jar which includes your `JobStatusChangedListener`, `JobStatusChangedListenerFactory`, `META-INF/services/` and all external dependencies.
Make a directory in `plugins/` of your Flink distribution with an arbitrary name, e.g. "job-status-changed-listener", and put the jar into this directory.
See [Flink Plugin]({{< ref "docs/deployment/filesystems/plugins" >}}) for more details.

JobStatusChangedListenerFactory example:

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

JobStatusChangedListener example:

``` java
package org.apache.flink.test.execution;

private static class TestingJobStatusChangedListener implements JobStatusChangedListener {

    @Override
    public void onEvent(JobStatusChangedEvent event) {
        statusChangedEvents.add(event);
    }
}
```

### Configuration

Flink components loads JobStatusChangedListener plugins at startup. To make sure your JobStatusChangedListeners are loaded all class names should be defined as part of [execution.job-status-changed-listeners]({{< ref "docs/deployment/config#execution.job-status-changed-listeners" >}}).
  If this configuration is empty, NO enrichers will be started. Example:
```
    execution.job-status-changed-listeners = org.apache.flink.test.execution.TestingJobStatusChangedListenerFactory
```

{{< top >}}
