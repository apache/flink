---
title: "Logging Context (MDC)"
weight: 9
type: docs
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

# Logging Context (MDC)

Flink populates the SLF4J [MDC](https://www.slf4j.org/api/org/slf4j/MDC.html) while handling jobs. Logging backends include MDC entries in log output, enabling log collectors to filter and group Flink logs by job without parsing message text. For details on rendering MDC entries with Log4j 2, see [Structured logging]({{< ref "docs/deployment/advanced/logging" >}}#structured-logging).

By default, the context holds a single entry:

| MDC key        | Value                                       |
|----------------|---------------------------------------------|
| `flink-job-id` | Job ID as a 32 character hexadecimal string |

Operators typically need more than a job ID to route logs, for example a tenant, a deployment name, or a pipeline name that persists across resubmissions. The `mdc.job-configuration-to-mdc-keys` option publishes job configuration entries to the MDC, so application code does not need to manage MDC entries directly.

## Configuration

{{< generated/mdc_configuration >}}

The value maps a job configuration key to the MDC key it is published under. Flink resolves the mapping when the job is submitted or recovered on the JobManager, and when a TaskManager accepts a task of that job. A configuration key that is absent from the job configuration, or whose value is blank, is skipped. `flink-job-id` is always present, and a mapping that targets `flink-job-id` is ignored.

The lookup runs against the job configuration, which is the cluster configuration from `config.yaml` merged with job-level configuration supplied at submission time (for example, `-D` arguments to `flink run`). Any key can be referenced, including keys that are not Flink configuration options.

## Example

Publish the pipeline name and an identifier that the operator injects at submission time:

```yaml
mdc.job-configuration-to-mdc-keys:
  pipeline.name: pipeline-name
  my.company.tenant-id: tenant-id
```

```bash
$ ./bin/flink run \
    -Dpipeline.name=nightly-aggregation \
    -Dmy.company.tenant-id=acme \
    ./examples/streaming/StateMachineExample.jar
```

Log records emitted for this job then carry three MDC entries:

```text
flink-job-id  = 4d1e3fbd4b1e4a4b8f9d0c6e2a7b5c31
pipeline-name = nightly-aggregation
tenant-id     = acme
```

JSON layouts that resolve the whole MDC pick the new fields up without further configuration. To include them in a plain text layout, extend the [Log4j 2 pattern]({{< ref "docs/deployment/advanced/logging" >}}#log4j-2-patternlayout), for example `[%X{flink-job-id}] [%X{tenant-id}] %c{0} %m%n`.

## Scope and lifetime

The enriched context lives in a process-local registry. The JobManager populates it when the Dispatcher submits or recovers the job, and each TaskManager populates it when it accepts a task of that job. Entries are dropped when the job reaches a terminal state on the JobManager, and when a TaskManager releases the resources of the job.

Before a job's configuration reaches a process, log records contain only `flink-job-id`. Client-side records produced while the job graph is being built are not scoped to any job.

## Notes

Values are read from the configuration that was submitted with the job. Changing `mdc.job-configuration-to-mdc-keys` or any mapped key while the job runs has no effect. Recovery reuses the stored job configuration, so cluster-level changes do not apply retroactively. Resubmit the job to pick up a new mapping.

Mapped values are written to log records as is. Do not map configuration keys that hold credentials or other secrets.

Every mapped key adds a field to every log record scoped to the job. Keep the mapping small to maintain predictable log volume and index cardinality.

{{< top >}}
