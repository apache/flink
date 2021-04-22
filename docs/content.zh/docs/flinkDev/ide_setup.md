---
title: "导入 Flink 到 IDE 中"
weight: 4
type: docs
aliases:
  - /zh/flinkDev/ide_setup.html
  - /zh/internals/ide_setup.html
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


# 导入 Flink 到 IDE 中

以下章节描述了如何将 Flink 项目导入到 IDE 中以进行 Flink 本身的源码开发。有关 Flink 程序编写的信息，请参阅 [Java API]({{< ref "docs/dev/datastream/project-configuration" >}}) 和 [Scala API]({{< ref "docs/dev/datastream/project-configuration" >}}) 快速入门指南。

{{< hint info >}}
每当你的 IDE 无法正常工作时，请优先尝试使用 Maven 命令行（`mvn clean package -DskipTests`），因为它可能是由于你的 IDE 中存在错误或未正确设置。
{{< /hint >}}


## 准备

首先，请从我们的[仓库](https://flink.apache.org/community.html#source-code)中拉取 Flink 源，例如：

```bash
git clone https://github.com/apache/flink.git
```


## 忽略重构提交

我们在 `.git-blame-ignore-revs` 中保留了一个大的重构提交列表。使用 `git blame` 查看更改注释时，忽略这些注释会很有帮助。你可以使用以下方法来配置 git 和你的 IDE：

```bash
git config blame.ignoreRevsFile .git-blame-ignore-revs
```


## IntelliJ IDEA

该指南介绍了关于如何设置 IntelliJ IDEA IDE 来进行 Flink 核心开发。众所周知由于 Eclipse 混合 Scala 和 Java 项目时存在问题，因此越来越多的贡献者正在迁移到 IntelliJ IDEA。

以下文档描述了 IntelliJ IDEA 2020.3 ([https://www.jetbrains.com/idea/download/](https://www.jetbrains.com/idea/download/)) 的设置步骤以及 Flink 的导入步骤。


### 安装 Scala 插件

IntelliJ 提供了插件设置来安装 Scala 插件。如果尚未安装，请在导入 Flink 之前按照以下说明来进行操作以启用对 Scala 项目和文件的支持：

1. 转到 IntelliJ Settings → Plugins 并选择 "Marketplace"。
2. 选择并安装 "Scala" 插件。
3. 如果出现提示，请重启 IntelliJ。


### 导入 Flink

1. 启动 IntelliJ IDEA 并选择 New → Project from Existing Sources。
2. 选择已克隆的 Flink 存储库的根文件夹。
3. 选择 "Import project from external model"，然后选择 "Maven"。
4. 保留默认选项，然后依次单击 "Next"，直到到达 SDK 部分。
5. 如果未列出 SDK，请使用左上角的 "+" 号创建一个。选择 "JDK"，选择 JDK 主目录，然后单击 "OK"。选择最合适的 JDK 版本。注意：一个好的经验法则是选择与活动 Maven 配置文件匹配的 JDK 版本。
6. 单击 "Next" 继续，直到完成导入。
7. 右键单击已导入的 Flink 项目 → Maven → Generate Sources and Update Folders。请注意：这会将 Flink 库安装在本地 Maven 存储库中，默认情况下位于 "/home/$USER/.m2/repository/org/apache/flink/"。另外 `mvn clean package -DskipTests` 也可以创建 IDE 运行所需的文件，但无需安装库。
8. 编译项目（Build → Make Project）。


### 代码格式化

我们使用 [Spotless plugin](https://github.com/diffplug/spotless/tree/main/plugin-maven) 和 [google-java-format](https://github.com/google/google-java-format) 一起格式化我们的 Java 代码。
                                                                                                                                                                                                                                              
你可以通过以下步骤来将 IDE 配置为在保存时自动应用格式设置：

1. 下载 [google-java-format plugin v1.7.0.6](https://plugins.jetbrains.com/plugin/8527-google-java-format/versions/stable/115957)
2. 打开 Settings → Plugins，点击齿轮图标并选择 "Install Plugin from Disk"。导航到下载的 zip 文件并选择它。
3. 在插件设置中，启用插件并将代码样式更改为 "AOSP"（4 个空格的缩进）。
4. 请记住不要将此插件更新为更高版本！
5. 安装 [Save Actions plugin](https://plugins.jetbrains.com/plugin/7642-save-actions)。
6. 启用插件，以及 "Optimize imports" 和 "Reformat file"。
7. 在 "Save Actions" 设置页面中，为 `.*\.java` 设置 "File Path Inclusion"。否则你将在编辑其他文件中意外的重新格式化。


### Java 规范检查

IntelliJ 使用 Checkstyle-IDEA 插件在 IDE 中支持 checkstyle。

1. 从 IntelliJ 插件存储库中安装 "Checkstyle-IDEA" 插件。
2. 通过 Settings → Tools → Checkstyle 配置插件。
3. 将 "Scan Scope" 设置为仅 Java 源（包括测试）。
4. 在 "Checkstyle Version" 下拉菜单中选择 _8.14_ 版本，然后单击 "apply"。**此步骤很重要，请勿跳过！**
5. 在 "Configuration File" 窗格中，点击 "+" 图标添加新配置：
    1. 将 "Description" 设置为 Flink。
    2. 选择 "Use a local Checkstyle file" ，然后将其指向你存储库中 `"tools/maven/checkstyle.xml"` 文件。
    3. 选中 "Store relative to project location" 框，然后单击 "Next"。
    4. 将 "checkstyle.suppressions.file" 属性值配置为 `"suppressions.xml"`，然后单击 "Next"，然后单击 "Finish"。
6. 选择 "Flink" 作为唯一的激活配置文件，单击 "Apply"，然后点击 "OK"。
7. Checkstyle 现在将在编辑器中针对任何违反 Checkstyle 的行为发出警告。

插件安装完成后你可以通过 Settings → Editor → Code Style → Java → Scheme 下拉框旁边的齿轮图标， 直接导入 `"tools/maven/checkstyle.xml"` 文件。 例如，这将自动调整导入布局。

你可以通过打开 Checkstyle 工具窗口并单击 "Check Module" 按钮来扫描整个模块。扫描不应报告任何错误。

{{< hint info >}}
存在一些模块没有完全被 CheckStyle 格式化，其中包括 `flink-core`，`flink-optimizer` 和 `flink-runtime`。不过，请确保你在这些模块中添加/修改的代码仍然符合 checkstyle 规则。
{{< /hint >}}


### Scala 规范检查

在 IntelliJ 中启用 scalastyle，通过选择 Settings → Editor → Inspections，然后搜索 "Scala style inspections"，还要放置 `"tools/maven/scalastyle-config.xml"` 在 `"<root>/.idea"` 或 `"<root>/project"` 目录中。


### 版权信息

每个文件都需要包含 Apache 许可证作为标头。这可以在 IntelliJ 中通过添加 Copyright Profile 来自动配置：

1. 打开 Settings → Editor → Copyright → Copyright Profiles。
2. 添加一个新的版权文件命名为 `Apache`。
3. 添加以下文本作为许可证文本：

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
4. 转到 Editor → Copyright 并选择 `Apache` 版权文件作为该项目的默认版权文件。
5. 点击 Apply，保存变更配置。


### 常问问题

本节列出了开发人员过去使用 IntelliJ 时遇到的问题：

- 编译失败 `invalid flag: --add-exports=java.base/sun.net.util=ALL-UNNAMED`

这意味着 IntelliJ 仍激活了 `Java 11` 配置文件，尽管使用了较旧的 JDK。解决方法：打开 Maven 工具窗口（View → tool Windows → Maven），取消选中 `Java 11` 配置文件并重新导入项目。

- 编译失败 `cannot find symbol: symbol: method defineClass(...) location: class sun.misc.Unsafe`

这意味着 IntelliJ 正在为该项目使用 JDK 11，但是你正在使用不支持 Java 11 的 Flink 版本。这通常在将 IntelliJ 设置为使用 JDK 11 并检出 Flink 的旧版本（<= 1.9）时发生。解决方法：打开项目设置窗口（File → Project Structure → Project Settings: Project），然后选择 JDK 8 作为项目 SDK。如果要使用 JDK 11，则可能必须在切换回新的 Flink 版本后恢复此状态。

- 运行 Flink Examples 且 Flink 出现关于 `NoClassDefFoundError` 错误信息

这可能是由于将 Flink 依赖项设置为 provided，导致它们没有自动放置在类路径中。你可以在运行配置中选中 "Include dependencies with 'Provided' scope" 框，也可以创建一个调用 `main()` 方法的测试示例（provided 依赖关系在测试类路径中可用）。


## Eclipse

{{< hint warning >}}
**注意:** 根据我们的经验，以下配置会导致 Flink 无法正常工作，如：由于与 Scala IDE 3.0.3 捆绑在一起的旧 Eclipse 版本的缺陷或者是由于 Scala IDE 4.4.1 中绑定的 Scala 版本不兼容。
{{< /hint >}}

**我们建议改为使用 IntelliJ（请参见[上文](#intellij-idea)）**


## PyCharm

关于如何为开发 flink-python 模块而设置 PyCharm 的简要指南。

以下文档介绍了使用 Flink Python 源设置 PyCharm 2019.1.3 ([https://www.jetbrains.com/pycharm/download/](https://www.jetbrains.com/pycharm/download/)) 的步骤。

### 导入 flink-python
如果你位于 PyCharm 启动界面中：

1. 启动 PyCharm，然后选择 "Open"。
2. 在克隆的 Flink 仓库中选择 flink-python 文件夹。

如果你使用 PyCharm 打开了一个项目：

1. 选择 File → Open。
2. 在克隆的 Flink 仓库中选择 flink-python 文件夹。


### Python 规范检查
Apache Flink 的 Python 代码检查样式应在项目中引入 flake8 的外部工具。

1. 将 flake8 安装在使用的 Python 解释器中（请参阅([https://pypi.org/project/flake8/](https://pypi.org/project/flake8/))）。
2. 选择 "PyCharm → Preferences... → Tools → External Tools → +（在右侧页面的左下角）"，然后配置外部工具。
3. 将 "Name" 设置为 "flake8"。
4. 将 "Description" 设置为 "code style check"。
5. 将 "Program" 设置为 Python 解释器路径（例如 /usr/bin/python）。
6. 将 "Arguments" 设置为 "-m flake8 \-\-config=tox.ini"。
7. 将 "Working directory" 设置为 "$ProjectFileDir$"。

现在，你可以通过以下操作来检查你的 Python 代码样式：
"右键单击 flink-python 项目中的任何文件或文件夹 → External Tools → flake8"。

{{< top >}}
