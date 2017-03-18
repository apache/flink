---
title: "Checkpoints"
nav-parent_id: setup
nav-pos: 7
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


* toc
{:toc}

## Overview

TBD


## Externalized Checkpoints

You can configure periodic checkpoints to be persisted externally. Externalized checkpoints write their meta data out to persistent storage and are *not* automatically cleaned up when the job fails. This way, you will have a checkpoint around to resume from if your job fails.

```java
CheckpointConfig config = env.getCheckpointConfig();
config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
```

The `ExternalizedCheckpointCleanup` mode configures what happens with externalized checkpoints when you cancel the job:

- **`ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION`**: Retain the externalized checkpoint when the job is cancelled. Note that you have to manually clean up the checkpoint state after cancellation in this case.

- **`ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION`**: Delete the externalized checkpoint when the job is cancelled. The checkpoint state will only be available if the job fails.

The **target directory** for the checkpoint is determined from the default checkpoint directory configuration. This is configured via the configuration key `state.checkpoints.dir`, which should point to the desired target directory:

```
state.checkpoints.dir: hdfs:///checkpoints/
```

This directory will then contain the checkpoint meta data required to restore the checkpoint. The actual checkpoint files will still be available in their configured directory. You currently can only set this via the configuration files.

Follow the [savepoint guide]({{ site.baseurl }}/setup/cli.html#savepoints) when you want to resume from a specific checkpoint.
