---
title: "Flink on Zeppelin QuickStart"
nav-title: Flink on Zeppelin Examples
nav-parent_id: examples
nav-pos: 31
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

* This will be replaced by the TOC
{:toc}

We provide a docker image to facilitate user to use Flink on Zeppelin easily.

## Requirements

Nothing else is required except docker, here's [reference](https://www.docker.com/get-started)

## How to use Flink in Zeppelin

Here's the command to launch zeppeln-blink image. (8085 is zeppelin port, 8091 is flink job mangager dashboard port)
```
docker run -d -p 8085:8085 -p 8091:8091 zjffdu/zeppelin-blink:latest
```

After executing this command, you can open http://localhost:8085 and we provide 3 tutorial in this image.

* Flink batch tutorial
* Flink stream tutorial
* Flink stream etl

And this is the command to ssh to this container in case you want to check the logs or something else.

```
docker exec -it <container_id> /bin/bash
```

We have one streaming ETL tutorial which require kafka installed. Here's kafka image we use to launch kafka service.
Check this [link](https://github.com/xushiyan/kafka-connect-datagen) for details
{% top %}
