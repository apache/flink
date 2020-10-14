---
title: "导入 Flink 到 IDE 中"
nav-parent_id: flinkdev
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

* Replaced by the TOC
{:toc}

以下章节描述了如何在 IDE 中导入 Flink 项目以此进行 Flink 本身的源码开发。若需要开发 Flink 应用程序，请参考 [Java API]({{ site.baseurl }}/zh/dev/project-configuration.html) 
和 [Scala API]({{ site.baseurl }}/zh/dev/project-configuration.html)的快速入门指南。

**注意：** 当你的 IDE 无法正常运行时，请优先尝试使用 Maven 命令 (`mvn clean package -DskipTests`) 因为这可能是你的 IDE 存在问题或设置不正确.

## 准备工作

首先，请先从[仓库](https://flink.apache.org/zh/community.html#source-code)中拉取 Flink 源码，例如
{% highlight bash %}
git clone https://github.com/apache/flink.git
{% endhighlight %}

## IntelliJ IDEA

该指南介绍了如何设置 IntelliJ IDEA IDE 以便进行 Flink 内核开发。
由于 Eclipse 在 Scala 和 Java 混合项目中存在问题，因此越来越多的贡献者倾向于使用 IntelliJ IDEA。

以下文档描述了 IntelliJ IDEA 2019.1.3 的设置步骤 ([https://www.jetbrains.com/idea/download/](https://www.jetbrains.com/idea/download/))


### 安装 Scala 插件

IntelliJ 的安装设置中支持安装 Scala 插件。如果尚未安装，请在导入 Flink 之前按照以下说明进行操作以便启用 IntelliJ 对 Scala 项目和文件的支持：

1. 进入 IntelliJ 插件设置后 (IntelliJ IDEA -> Preferences -> Plugins) 点击 "Install Jetbrains plugin..."。
2. 选择并安装 "Scala" 插件。
3. 重启 IntelliJ。

### 导入 Flink

1. 启动 IntelliJ IDEA 并选择 "New -> Project from Existing Sources"
2. 选择目录用于存放克隆的 Flink 仓库 
3. 选择 "Import project from external model" 后再选择 "Maven"
4. 使用默认配置并连续点击 "Next" 一直到达设置 SDK 的部分
5. 如果没有可选的 SDK 列表，请使用左上方的 "+" 号进行创建。
   选择 "JDK"，再选择 JDK 主目录并点击 "OK"。
   此处应选择最合适的 JDK 版本。 注意：较好选择 JDK 版本的经验是选择与当前 Maven 的配置文件相匹配的版本
6. 连续点击 "Next" 直到完成导入
7. 在导入的 Flink 项目上单击右键，点击 "Maven -> Generate Sources and Update Folders"。
   请注意这会在你本地的 Maven 仓库中安装 Flink 的依赖库，默认的位置为 "/home/$USER/.m2/repository/org/apache/flink/"。
   或者 `mvn clean package -DskipTests` 也会创建 IDE 运行所需的文件但是不会安装依赖库。
8. 编译项目 (Build -> Make Project)

### Java Checkstyle
IntelliJ 可以使用 Checkstyle-IDEA 插件进行代码风格检查。

1. 从 IntelliJ 插件库中选择安装 "Checkstyle-IDEA" 插件。
2. 可通过 "Settings -> Other Settings -> Checkstyle" 对插件进行配置。
3. 在 "Scan Scope" 中选择 "Only Java sources (including tests)"。
4. 在 "Checkstyle Version" 下拉框中选择 _8.14_ 版本。 **这一步设置很重要，请不要忽略！**
5. 在 "Configuration File" 面板，使用 "+" 图标添加新配置：
    1. 在 "Description" 中填写 "Flink"。
    2. 选中 "Use a local Checkstyle file"， 然后在你的仓库中选择 `"tools/maven/checkstyle.xml"`。
    3. 勾选 "Store relative to project location"，然后点击 "Next"。
    4. 赋值 "checkstyle.suppressions.file" 项为 `"suppressions.xml"`，然后点击 "Next", 接着点击 "Finish"。
6. 选择 "Flink" 作为唯一有效的配置文件，然后点击 "Apply" 和 "OK"。
7. 至此 Checkstyle 将在编辑器中对于任何违反 Checkstyle 规范的行为发出警告。

安装插件后你可以直接导入 `"tools/maven/checkstyle.xml"` 文件，导入入口在 Settings -> Editor -> Code Style -> Java -> 点击齿轮按钮出现 Scheme 下拉框。这将自动调整导入布局。

你可以通过打开 Checkstyle 工具窗口并单击 "Check Module" 按钮来扫描整个模块。扫描结果不应报告任何错误。

<span class="label label-info">注意</span> 某些模块未被 checkstyle 完全覆盖，包括 `flink-core`，`flink-optimizer` 和 `flink-runtime`。
然而请确保你在这些模块中添加/修改的代码仍然符合 checkstyle 规则。

### Scala Checkstyle

在 Intellij 中启用 scalastyle 可通过选择 Settings -> Editor -> Inspections，然后搜索 "Scala style inspections"。 也可以直接在 `"<root>/.idea"` 或 `"<root>/project"` 目录中替换 `"tools/maven/scalastyle-config.xml"` 文件。

### 版权信息

每个文件的开头都需要申明 Apache 许可证。这可通过在 IntelliJ 中添加 Copyright Profile 进行自动配置：
1. 打开 IntelliJ 配置
2. 点击 Editor -> Copyright -> Copyright Profiles
3. 新增名为 `Apache` 的概述
4. 使用以下文本作为许可证的内容：
    
   ```
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at
   
       http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and 
   limitations under the License.
   ```
5. 点击 Apply 保存变更配置

### 常见问题

本节列出了开发人员过去使用 IntelliJ 时遇到的问题：

- 编译失败并出现 `invalid flag: --add-exports=java.base/sun.net.util=ALL-UNNAMED`

这表明 IntelliJ 虽然使用了较旧版本的 JDK，但是同时使用了 `java11` 的配置文件。
打开 Maven 工具栏 (View -> Tool Windows -> Maven)， 在 Profiles下取消选中 `java11` 并点击 Reimport 重新导入项目。

- 编译失败并出现 `cannot find symbol: symbol: method defineClass(...) location: class sun.misc.Unsafe`

这表明 IntelliJ 中项目使用的JDK 版本是 JDK 11，但是你使用的 Flink 版本不支持 Java 11。
通常会发生这种情况是因为在 IntelliJ 中设置了使用 JDK 11，但是使用的 Flink 版本过低导致的 (<= 1.9)。
打开项目配置窗口 (File -> Project Structure -> Project Settings: Project) 并选择 JDK 8 作为项目的 SDK。
当切换到 Flink 新版本并且希望使用 JDK 11 时则可能需要重新设置 SDK。

- 运行 Flink Examples 时出现关于 Flink 相关类的 `NoClassDefFoundError`

这可能是由于将 Flink 的依赖设置成了 `provided`，导致依赖没有在类路径中被自动加载。
可以在运行配置中勾选 "Include dependencies with 'Provided' scope"，或新建可以调用 Example 类的 `main()` 方法的测试 (`provided` 相关的依赖存在于测试的类加载路径).

## Eclipse

**注意:** 根据我们的经验，由于捆绑 Scala IDE 3.0.3 的旧版本 Eclipse 存在缺陷或由于捆绑 Scala IDE 4.4.1 的 Eclipse 存在版本不兼容问题，
因此 Eclipse 不适用于 Flink。

**我们建议使用 IntelliJ 来代替 Eclipse (见 [IntelliJ](#intellij-idea))**

## PyCharm

该指南介绍了如何设置 PyCharm 以便进行 flink-python 模块的开发。

以下文档介绍了 PyCharm 2019.1.3 的安装步骤
([https://www.jetbrains.com/pycharm/download/](https://www.jetbrains.com/pycharm/download/))

### 导入 flink-python
如果你正位于 PyCharm 的启动界面中：

1. 启动 PyCharm 并选择 "Open"
2. 在克隆的 Flink 源码仓库中选择 flink-python 文件夹

如果你已经使用 PyCharm 打开了项目：

1. 选择 "File -> Open"
2. 在克隆的 Flink 源码仓库中选择 flink-python 文件夹


### Python Checkstyle 
Apache Flink 的 Python 代码风格检查需在项目中创建 flake8 外部工具。 

1. 在已使用的 Python 解释器中安装 flake8 (参考 ([https://pypi.org/project/flake8/](https://pypi.org/project/flake8/))).
2. 选择 "PyCharm -> Preferences... -> Tools -> External Tools -> + (位于右侧窗口的左下角)"，然后开始配置外部工具。
3. 将 "Name" 设置为 "flake8"。
4. 将 "Description" 设置为 "code style check"。
5. 将 "Program" 设置为 Python 解释器路径 (如 /usr/bin/python)。
6. 将 "Arguments" 设置为 "-m flake8 \-\-config=tox.ini"
7. 将 "Working directory" 设置为 "$ProjectFileDir$"

现在，你可以通过以下操作进行 Python 的代码风格检查：
    "右键单击 flink-python 项目中的任何文件或文件夹 -> External Tools -> flake8"
    
{% top %}
