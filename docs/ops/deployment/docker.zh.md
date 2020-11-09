---
title:  "Docker 设置"
nav-title: Docker
nav-parent_id: deployment
nav-pos: 6
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

[Docker](https://www.docker.com) 是一种流行的容器引擎。
可以在 [Docker Hub](https://hub.docker.com/_/flink) 上获取 Apache Flink 可用的 Docker 镜像。
你可以使用 docker 镜像在容器环境中，例如 [standalone Kubernetes](kubernetes.html) 或 [native Kubernetes](native_kubernetes.html) 中部署 *Session 集群* 或 *Job 集群*。

* This will be replaced by the TOC
{:toc}

## Docker Hub 里的 Flink 镜像

[Flink Docker](https://hub.docker.com/_/flink/) 仓库托管在 Docker Hub，并提供 Flink 1.2.1 及更高版本的镜像。

<a name="image-tags"></a>

### 镜像标记

任意受支持的 Flink 和 Scala 版本组合的镜像均是可用的，并提供 [tag aliases](https://hub.docker.com/_/flink?tab=tags) 以方便使用。

例如，你可以使用以下标记:

* `flink:latest` → `flink:<latest-flink>-scala_<latest-scala>`
* `flink:1.11` → `flink:1.11.<latest-flink-1.11>-scala_2.11`

<span class="label label-info">注意</span> 建议始终使用 docker 镜像的显式版本标记，指定所需的 Flink 和 Scala 版本号(例如 `flink:1.11-scala_2.12`)。
避免当应用程序中使用的 Flink 和(或) Scala 版本与 docker 镜像提供的版本不同时可能引发的类冲突。

<span class="label label-info">注意</span> 在 Flink 1.5 版本之前，Hadoop 依赖总是和 Flink 捆绑在一起。
你可以看到某些标签包含了 Hadoop 的版本(例如: `-hadoop28`)。
从 Flink 1.5 版本开始，省略 Hadoop 版本的镜像标记对应 Flink 的 Hadoop-free 版本，即不包含捆绑的 Hadoop 发行版。

<a name="how-to-run-a-flink-image"></a>

## 如何运行 Flink 镜像

Flink 镜像包含具有默认配置的常规 Flink 发行版和标准的 entry point 脚本。
你可以在如下模式下运行它的 entry point:
* [Session 集群](#start-a-session-cluster) 的 [JobManager]({% link concepts/glossary.zh.md %}#flink-jobmanager)
* [Job 集群](#start-a-job-cluster) 的 [JobManager]({% link concepts/glossary.zh.md %}#flink-jobmanager)
* 任意集群的 [TaskManager]({% link concepts/glossary.zh.md %}#flink-taskmanager)

这允许你在任何容器环境中部署一个独立集群 (Session 或 Job)，例如:
* 本地 Docker 环境中手动部署
* [Kubernetes cluster](kubernetes.html)
* [Docker Compose](#flink-with-docker-compose)
* [Docker swarm](#flink-with-docker-swarm)

<span class="label label-info">注意</span> [The native Kubernetes](native_kubernetes.html) 默认情况下也运行相同的镜像，并按需部署 *TaskManagers* ，因此你不必手动执行。

接下来的章节将描述如何启动一个用于多种目的的 Flink Docker 容器。

一旦你在 Docker 上启动了 Flink，你可以通过访问 Flink 的前端页面 [localhost:8081](http://localhost:8081/#/overview)，或者通过 `./bin/flink run ./examples/streaming/TopSpeedWindowing.jar` 命令来提交作业。

我们建议使用 [Docker Compose](docker.html#session-cluster-with-docker-compose) 或 [Docker Swarm](docker.html#session-cluster-with-docker-swarm) 将 Flink 部署为 Session 集群，以简化系统配置。

<a name="start-a-session-cluster"></a>

### 启动 Session 集群

*Flink Session 集群* 可用于运行多个作业。每个作业都必须在集群部署之后才可以提交。
要使用 Docker 部署 *Flink Session 集群*，你需要启动 *JobManager* 容器，并确保容器间可通信，首先，设置必需的 Flink 配置属性，并创建一个网络：
```sh
FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager"
docker network create flink-network
```

然后启动 JobManager:

```sh
docker run \
    --rm \
    --name=jobmanager \
    --network flink-network \
    -p 8081:8081 \
    --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
    flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} jobmanager
```

和一个或多个 *TaskManager* 容器:

```sh
docker run \
    --rm \
    --name=taskmanager \
    --network flink-network \
    --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
    flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} taskmanager
```

<a name="start-a-job-cluster"></a>

### 启动 Job 集群

*Flink Job 集群*是运行单个作业的专用集群。
这种情况下，包含作业的集群部署将在一个步骤中完成，因此不需要额外的作业提交过程。

*job artifacts* 包含在容器内 Flink 的 JVM 进程的类路径中，包括：
* 你的作业 jar 包，该 jar 包通常会提交给 *Session 集群*
* 所有未包含在 Flink 中的必要依赖和资源。

要使用 Docker 为单个作业部署集群，你需要
* 将 *job artifacts* 放在所有容器的 `/opt/flink/usrlib` 目录下以保证本地可用,
* 在 *Job 集群*模式下启动一个 *JobManager* 容器
* 启动所需数量的 *TaskManager* 容器。

要使 **job artifacts** 在容器中本地可用，你可以

* 当你启动 *JobManager* 和 *TaskManagers* 时，可以将**包含依赖的一个卷**(或多个卷)挂载到 `/opt/flink/usrlib` 目录下。

    ```sh
    FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager"
    docker network create flink-network

    docker run \
        --mount type=bind,src=/host/path/to/job/artifacts1,target=/opt/flink/usrlib/artifacts1 \
        --mount type=bind,src=/host/path/to/job/artifacts2,target=/opt/flink/usrlib/artifacts2 \
        --rm \
        --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
        --name=jobmanager \
        --network flink-network \
        flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} standalone-job \
        --job-classname com.job.ClassName \
        [--job-id <job id>] \
        [--fromSavepoint /path/to/savepoint [--allowNonRestoredState]] \
        [job arguments]

    docker run \
        --mount type=bind,src=/host/path/to/job/artifacts1,target=/opt/flink/usrlib/artifacts1 \
        --mount type=bind,src=/host/path/to/job/artifacts2,target=/opt/flink/usrlib/artifacts2 \
        --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
        flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} taskmanager
    ```

* 或者通过编写自定义的 `Dockerfile` 文件来**扩展 Flink 镜像**，构建并使用它来启动 *JobManager* 和 *TaskManagers*:

    *Dockerfile*:

    ```dockerfile
    FROM flink
    ADD /host/path/to/job/artifacts/1 /opt/flink/usrlib/artifacts/1
    ADD /host/path/to/job/artifacts/2 /opt/flink/usrlib/artifacts/2
    ```

    ```sh
    docker build -t flink_with_job_artifacts .
    docker run \
        flink_with_job_artifacts standalone-job \
        --job-classname com.job.ClassName \
        [--job-id <job id>] \
        [--fromSavepoint /path/to/savepoint [--allowNonRestoredState]] \
        [job arguments]

    docker run flink_with_job_artifacts taskmanager
    ```

`standalone-job` 参数会在 *Job 集群* 模式下启动一个 *JobManager* 容器。

<a name="jobmanager-additional-command-line-arguments"></a>

#### JobManager 的附加命令行参数

你可以在集群 entrypoint 处提供以下附加命令行参数：

* `--job-classname <job class name>`: 将运行的作业类名。

  默认情况下，Flink 在其类路径中扫描含有 Main-Class 或 program-class 清单条目的 JAR 包，并选择它作为作业类。
  使用该命令行可以手动设置作业类。
  若类路径中没有或不止一个带有此类清单条目的 Jar 包可用，则需要该参数。

* `--job-id <job id>` (可选)：手动设置 Flink 作业 ID (默认值：00000000000000000000000000000000)

* `--fromSavepoint /path/to/savepoint` (可选)： 从保存点还原

  为了从保存点恢复，还需要传递保存点路径。
  请注意，需要保证集群中的所有 Docker 容器均可以访问 `/path/to/savepoint` 路径(例如，将其存储在 DFS 或挂载卷中或将其添加到镜像中)。

* `--allowNonRestoredState` (可选)： 跳过损坏的保存点状态

  你可以指定该参数以允许跳过无法恢复的保存点状态。

如果用户作业主类的主函数需要传递参数，你也可以追加在 `docker run` 命令后面。

## 自定义 Flink 镜像

运行 Flink 容器时，可能需要自定义它们。下一章将介绍一些可以自定义的方法。

### 配置选项

运行 Flink 镜像时，还可以通过设置环境变量 `FLINK_PROPERTIES` 来更改其配置选项:

```sh
FLINK_PROPERTIES="jobmanager.rpc.address: host
taskmanager.numberOfTaskSlots: 3
blob.server.port: 6124
"
docker run --env FLINK_PROPERTIES=${FLINK_PROPERTIES} flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} <jobmanager|standalone-job|taskmanager>
```

必须配置 [`jobmanager.rpc.address`](../config.html#jobmanager-rpc-address) 选项，其他为可选配置。

和 `flink-conf.yaml` 类似，环境变量 `FLINK_PROPERTIES` 需要包含 Flink 集群的配置选项列表，并以换行符分隔。`FLINK_PROPERTIES` 中的配置优先于 `flink-conf.yaml`。

### 提供自定义配置

配置文件(`flink-conf.yaml`、logging、hosts 等)位于 Flink 镜像的 `/opt/flink/conf` 目录下。
若要为 Flink 配置文件提供自定义的位置，则可以

* 运行 Flink 镜像时，将含有自定义配置文件的**卷挂载**到 `/opt/flink/conf` 目录下:

    ```sh
    docker run \
        --mount type=bind,src=/host/path/to/custom/conf,target=/opt/flink/conf \
        flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} <jobmanager|standalone-job|taskmanager>
    ```

* 或者将它们添加到你的**自定义 Flink 镜像**中，构建并运行它:

    *Dockerfile*:

    ```dockerfile
    FROM flink
    ADD /host/path/to/flink-conf.yaml /opt/flink/conf/flink-conf.yaml
    ADD /host/path/to/log4j.properties /opt/flink/conf/log4j.properties
    ```

<span class="label label-warning">警告!</span> 挂载卷需要包含所有必需的配置文件。
`flink-conf.yaml` 必须具有写权限，以便 Docker entry point 脚本可以在某些情况下对其进行修改。

### 使用插件

如 [plugins]({{ site.baseurl }}/zh/ops/plugins.html) 文档中所述：为了使用插件，必须将它们复制到 Docker 容器中 Flink 安装目录下的正确位置才能起作用。

如果要启用 Flink 自带的插件(在 Flink 发行版的 `opt/` 目录下)，则可以在运行 Flink 镜像时传递环境变量 `ENABLE_BUILT_IN_PLUGINS`。
`ENABLE_BUILT_IN_PLUGINS` 应包含一个以 `;` 分隔的插件列表。举一个有效的插件样例如 `flink-s3-fs-hadoop-{{site.version}}.jar`。

```sh
    docker run \
        --env ENABLE_BUILT_IN_PLUGINS=flink-plugin1.jar;flink-plugin2.jar \
        flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} <jobmanager|standalone-job|taskmanager>
```

还有更多更[高级的方法](#advanced-customization)来定制 Flink 镜像。

<a name="advanced-customization"></a>

### 高级定制

你可以通过下述方法进一步自定义 Flink 图像:

* 安装自定义软件(例如 python)
* 从 `/opt/flink/opt` 启用(符号链接)可选库或插件到 `/opt/flink/lib` 或 `/opt/flink/plugins`
* 将其他库添加到 `/opt/flink/lib` 目录下(例如 Hadoop)
* 将其他插件添加到 `/opt/flink/plugins` 目录下

也可以参阅: [如何在类路径中添加依赖项]({% link index.zh.md %}#how-to-provide-dependencies-in-the-classpath).

可以通过下述方式自定义 Flink 镜像:

* 你可以使用自定义脚本**覆盖容器的 entry point**，然后在其中运行任何的引导操作。
最后，使用[如何运行 Flink 镜像](#how-to-run-a-flink-image)中所述的相同的参数，调用 Flink 镜像标准的 `/docker-entrypoint.sh` 脚本。

  以下示例创建了一个自定义 entry point 脚本，该脚本启用了更多的库和插件。
  自定义脚本、自定义库和自定义插件均是已挂载的卷提供的。
  然后该自定义脚本会运行 Flink 镜像的标准 entry point 脚本:

    ```sh
    # create custom_lib.jar
    # create custom_plugin.jar

    echo "
    ln -fs /opt/flink/opt/flink-queryable-state-runtime-*.jar /opt/flink/lib/.  # enable an optional library
    ln -fs /mnt/custom_lib.jar /opt/flink/lib/.  # enable a custom library

    mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop
    ln -fs /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/flink-s3-fs-hadoop/.  # enable an optional plugin

    mkdir -p /opt/flink/plugins/custom_plugin
    ln -fs /mnt/custom_plugin.jar /opt/flink/plugins/custom_plugin/.  # enable a custom plugin

    /docker-entrypoint.sh <jobmanager|standalone-job|taskmanager>
    " > custom_entry_point_script.sh

    chmod 755 custom_entry_point_script.sh

    docker run \
        --mount type=bind,src=$(pwd),target=/mnt
        flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} /mnt/custom_entry_point_script.sh
    ```

* 通过编写自定义 `Dockerfile` 文件来**扩展 Flink 镜像**，并构建自定义镜像:

    *Dockerfile*:

    ```dockerfile
    FROM flink

    RUN set -ex; apt-get update; apt-get -y install python

    ADD /host/path/to/flink-conf.yaml /container/local/path/to/custom/conf/flink-conf.yaml
    ADD /host/path/to/log4j.properties /container/local/path/to/custom/conf/log4j.properties

    RUN ln -fs /opt/flink/opt/flink-queryable-state-runtime-*.jar /opt/flink/lib/.

    RUN mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop
    RUN ln -fs /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/flink-s3-fs-hadoop/.

    ENV VAR_NAME value
    ```

    **打包命令**:

    ```sh
    docker build -t custom_flink_image .
    # optional push to your docker image registry if you have it,
    # e.g. to distribute the custom image to your cluster
    docker push custom_flink_image
    ```

{% top %}

<a name="flink-with-docker-compose"></a>

## Flink 和 Docker Compose

[Docker Compose](https://docs.docker.com/compose/) 是一种在本地运行一组 Docker 容器的方法。
下一章将展示用于运行 Flink 的配置文件示例。

### 用法

* 使用容器配置创建 `yaml` 文件，请查看以下示例:
    * [Session 集群](#session-cluster-with-docker-compose)
    * [Job 集群](#job-cluster-with-docker-compose)

    也可以参阅 [Flink Docker 镜像标记](#image-tags) 和 [如何自定义 Flink Docker 镜像](#advanced-customization)获取在配置文件中的用法。

* 在前台启动集群

    ```sh
    docker-compose up
    ```

* 在后台启动集群

    ```sh
    docker-compose up -d
    ```

* 将集群向上或向下扩展到 *N TaskManagers*

    ```sh
    docker-compose scale taskmanager=<N>
    ```

* 访问 *JobManager* 容器

    ```sh
    docker exec -it $(docker ps --filter name=jobmanager --format={% raw %}{{.ID}}{% endraw %}) /bin/sh
    ```

* 杀掉集群

    ```sh
    docker-compose kill
    ```

* 访问 Web UI

    当集群运行时，你可以访问 [http://localhost:8081](http://localhost:8081) 上的 Web UI。
    还可以使用 Web UI 提交一个作业到 *Session 集群*。

* 要通过命令行提交作业到 *Session 集群*，你可以

  * 若主机上已安装 [Flink CLI](..//cli.html)，则通过:

    ```sh
    flink run -d -c ${JOB_CLASS_NAME} /job.jar
    ```

  * 或者将 JAR 复制到 *JobManager* 容器，然后使用 [CLI](..//cli.html) 从此处提交作业，例如:

    ```sh
    JOB_CLASS_NAME="com.job.ClassName"
    JM_CONTAINER=$(docker ps --filter name=jobmanager --format={% raw %}{{.ID}}{% endraw %}))
    docker cp path/to/jar "${JM_CONTAINER}":/job.jar
    docker exec -t -i "${JM_CONTAINER}" flink run -d -c ${JOB_CLASS_NAME} /job.jar
    ```

<a name="session-cluster-with-docker-compose"></a>

### Session 集群和 Docker Compose

**docker-compose.yml:**

```yaml
version: "2.2"
services:
  jobmanager:
    image: flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %}
    ports:
      - "8081:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager

  taskmanager:
    image: flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %}
    depends_on:
      - jobmanager
    command: taskmanager
    scale: 1
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 2
```

<a name="job-cluster-with-docker-compose"></a>

### Job 集群和 Docker Compose

插件必须在 Flink 容器中可用，在[此处](#start-a-job-cluster)查看详细消息。
也可以参阅[如何指定 JobManager 参数](#jobmanager-additional-command-line-arguments)获取如何配置 `jobmanager` 服务的`命令行`。

**docker-compose.yml:**

```yaml
version: "2.2"
services:
  jobmanager:
    image: flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %}
    ports:
      - "8081:8081"
    command: standalone-job --job-classname com.job.ClassName [--job-id <job id>] [--fromSavepoint /path/to/savepoint [--allowNonRestoredState]] [job arguments]
    volumes:
      - /host/path/to/job/artifacts:/opt/flink/usrlib
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        parallelism.default: 2

  taskmanager:
    image: flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %}
    depends_on:
      - jobmanager
    command: taskmanager
    scale: 1
    volumes:
      - /host/path/to/job/artifacts:/opt/flink/usrlib
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 2
        parallelism.default: 2
```

{% top %}

<a name="flink-with-docker-swarm"></a>

## Flink 和 Docker Swarm

[Docker swarm](https://docs.docker.com/engine/swarm) 是一个容器编排工具，允许你跨多个主机部署并管理多个容器。

下面章节包含有关如何配置和启动 *JobManager* 和 *TaskManager* 的示例。
你可以相应的调整它们来启动集群。
也可以参阅[Flink Docker 镜像标记](#image-tags) 和 [如何自定义 Flink Docker 镜像](#advanced-customization)获取在配置文件中的用法。

`8081` 端口被暴露出来以供 Flink Web UI 访问。
如果你在本地运行 swarm，你可以在启动集群后，在 [http://localhost:8081](http://localhost:8081) 上访问 Web UI。

<a name="session-cluster-with-docker-swarm"></a>

### Session 集群和 Docker Swarm

```sh
FLINK_PROPERTIES="jobmanager.rpc.address: flink-session-jobmanager
taskmanager.numberOfTaskSlots: 2
"

# Create overlay network
docker network create -d overlay flink-session

# Create the JobManager service
docker service create \
  --name flink-session-jobmanager \
  --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
  -p 8081:8081 \
  --network flink-session \
  flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} \
    jobmanager

# Create the TaskManager service (scale this out as needed)
docker service create \
  --name flink-session-taskmanager \
  --replicas 2 \
  --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
  --network flink-session \
  flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} \
    taskmanager
```

### Job 集群和 Docker Swarm

```sh
FLINK_PROPERTIES="jobmanager.rpc.address: flink-jobmanager
taskmanager.numberOfTaskSlots: 2
"

# Create overlay network
docker network create -d overlay flink-job

# Create the JobManager service
docker service create \
  --name flink-jobmanager \
  --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
  --mount type=bind,source=/host/path/to/job/artifacts,target=/opt/flink/usrlib \
  -p 8081:8081 \
  --network flink-job \
  flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} \
    standalone-job \
    --job-classname com.job.ClassName \
    [--job-id <job id>] \
    [--fromSavepoint /path/to/savepoint [--allowNonRestoredState]] \
    [job arguments]

# Create the TaskManager service (scale this out as needed)
docker service create \
  --name flink-job-taskmanager \
  --replicas 2 \
  --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
  --mount type=bind,source=/host/path/to/job/artifacts,target=/opt/flink/usrlib \
  --network flink-job \
  flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} \
    taskmanager
```

如[此处](#start-a-job-cluster)所述，*作业插件*必须在 *JobManager* 容器中可用。
也可以参阅[如何指定 JobManager 参数](#jobmanager-additional-command-line-arguments)，并将其传递给 `flink-jobmanager` 容器。

该示例假定你在本地运行 swarm，且*作业插件*位于 `/host/path/to/job/artifacts` 目录下。
同时将带有插件的主机路径作为卷挂载到容器的 `/opt/flink/usrlib` 目录下。

{% top %}
