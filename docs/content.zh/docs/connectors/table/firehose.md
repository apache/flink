---
title: Firehose
weight: 5
type: docs
aliases:
- /dev/table/connectors/firehose.html
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

# Amazon Kinesis Data Firehose SQL 连接器

{{< label "数据 sink: 流追加模式" >}}

Kinesis Data Firehose 连接器允许将数据写入 Amazon Kinesis Data Firehose [Amazon Kinesis Data Firehose (KDF)](https://aws.amazon.com/kinesis/data-firehose/)。

依赖
------------

{{< sql_download_table "aws-kinesis-firehose" >}}

如何创建 Kinesis Data Firehose 表
-----------------------------------------

按照下面链接的指引创建 Kinesis Data Firehose 数据传输流 [Amazon Kinesis Data Firehose 开发者向导](https://docs.aws.amazon.com/ses/latest/dg/event-publishing-kinesis-analytics-firehose-stream.html)。  
下面的示例显示了如何创建一个由 Kinesis Data Firehose 传输流支持的表，该传输流具有所需的最少选项：

```sql
CREATE TABLE FirehoseTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `category_id` BIGINT,
  `behavior` STRING
)
WITH (
  'connector' = 'firehose',
  'delivery-stream' = 'user_behavior',
  'aws.region' = 'us-east-2',
  'format' = 'csv'
);
```

连接器选项
-----------------

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 25%">选项</th>
      <th class="text-center" style="width: 8%">是否必须配置</th>
      <th class="text-center" style="width: 7%">缺省值</th>
      <th class="text-center" style="width: 10%">类型</th>
      <th class="text-center" style="width: 50%">描述</th>
    </tr>
    <tr>
      <th colspan="5" class="text-left" style="width: 100%">通用选项</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>connector</h5></td>
      <td>是</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定要使用的连接器。 使用 Kinesis Data Firehose 则配置为 <code>'firehose'</code>.</td>
    </tr>
    <tr>
      <td><h5>delivery-stream</h5></td>
      <td>是</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>支持此表的 Kinesis Data Firehose 数据传输流的名称。</td>
    </tr>
    <tr>
      <td><h5>format</h5></td>
      <td>是</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>"format" 用于对 Kinesis Data Firehose 结果集做反序列化和序列化。详情可以参考本页 <a href="#数据类型映射">数据类型映射</a> 。</td>
    </tr>
    <tr>
      <td><h5>aws.region</h5></td>
      <td>是</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>定义传输流的 AWS 区域。创建 <code>KinesisFirehoseSink</code> 时需要此选项。</td>
    </tr>
    <tr>
      <td><h5>aws.endpoint</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Amazon Kinesis Data Firehose 的 AWS 端点。</td>
    </tr>
    <tr>
      <td><h5>aws.trust.all.certificates</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>如果为 true 则接受所有 SSL 证书。</td>
    </tr>
    </tbody>
    <thead>
    <tr>
      <th colspan="5" class="text-left" style="width: 100%">身份验证选项</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>aws.credentials.provider</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">AUTO</td>
      <td>String</td>
      <td>针对 Kinesis 端点进行身份验证时要使用的凭据提供程序。详情参考 <a href="#身份验证">身份验证</a> 。</td>
    </tr>
    <tr>
   <td><h5>aws.credentials.basic.accesskeyid</h5></td>
   <td>可选</td>
   <td style="word-wrap: break-word;">(none)</td>
   <td>String</td>
   <td>将凭据提供程序类型设置为 BASIC 时要使用的 AWS 访问密钥 ID 。</td>
    </tr>
    <tr>
   <td><h5>aws.credentials.basic.secretkey</h5></td>
   <td>可选</td>
   <td style="word-wrap: break-word;">(none)</td>
   <td>String</td>
   <td>将凭据提供程序类型设置为 BASIC 时使用的 AWS 密钥。</td>
    </tr>
    <tr>
   <td><h5>aws.credentials.profile.path</h5></td>
   <td>可选</td>
   <td style="word-wrap: break-word;">(none)</td>
   <td>String</td>
   <td>如果凭据提供程序类型设置为配置文件，则配置为配置文件路径。</td>
    </tr>
    <tr>
   <td><h5>aws.credentials.profile.name</h5></td>
   <td>可选</td>
   <td style="word-wrap: break-word;">(none)</td>
   <td>String</td>
   <td>如果凭据提供程序类型设置为配置文件，则配置为配置文件的名称。</td>
    </tr>
    <tr>
   <td><h5>aws.credentials.role.arn</h5></td>
   <td>可选</td>
   <td style="word-wrap: break-word;">(none)</td>
   <td>String</td>
   <td>当凭据提供者类型设置为 ASSUME_ROLE 或者 WEB_IDENTITY_TOKEN 时，设置 ARN 角色。</td>
    </tr>
    <tr>
   <td><h5>aws.credentials.role.sessionName</h5></td>
   <td>可选</td>
   <td style="word-wrap: break-word;">(none)</td>
   <td>String</td>
   <td>当凭证提供程序类型设置为 ASSUME_ROLE 或 WEB_IDENTITY_TOKEN 的时候要使用角色会话名称。</td>
    </tr>
    <tr>
   <td><h5>aws.credentials.role.externalId</h5></td>
   <td>可选</td>
   <td style="word-wrap: break-word;">(none)</td>
   <td>String</td>
   <td>当凭证提供程序类型设置为 ASSUME_ROLE 的时候要使用的外部 ID 。</td>
    </tr>
    <tr>
   <td><h5>aws.credentials.role.provider</h5></td>
   <td>可选</td>
   <td style="word-wrap: break-word;">(none)</td>
   <td>String</td>
   <td>当凭证提供程序类型设置为 ASSUME_ROLE 类型时，凭证提供程序提供凭证角色。因为角色可以嵌套，所以可以再次将类型设置为 ASSUME_ROLE 。</td>
    </tr>
    <tr>
   <td><h5>aws.credentials.webIdentityToken.file</h5></td>
   <td>可选</td>
   <td style="word-wrap: break-word;">(none)</td>
   <td>String</td>
   <td>如果提供程序类型设置为 WEB_IDENTITY_TOKEN ，则应使用的站点标识令牌文件的绝对路径。</td>
    </tr>
    </tbody>
    <thead>
    <tr>
      <th colspan="5" class="text-left" style="width: 100%">Sink 选项</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>sink.http-client.max-concurrency</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">10000</td>
      <td>Integer</td>
      <td>
      <code>FirehoseAsyncClient</code> 设置允许当前请求传递到传输流的最大并发数。
      </td>
    </tr>
    <tr>
      <td><h5>sink.http-client.read-timeout</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">360000</td>
      <td>Integer</td>
      <td>
        <code>FirehoseAsyncClient</code> 在失败前向传输流发送请求的最长时间（毫秒）。
      </td>
    </tr>
    <tr>
      <td><h5>sink.http-client.protocol.version</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">HTTP2</td>
      <td>String</td>
      <td><code>FirehoseAsyncClient</code> 使用的 HTTP 报文的版本号。</td>
    </tr>
    <tr>
      <td><h5>sink.batch.max-size</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">500</td>
      <td>Integer</td>
      <td><code>FirehoseAsyncClient</code> 允许一个批处理可以写到下游的传输流的最大批次记录数。</td>
    </tr>
    <tr>
      <td><h5>sink.requests.max-inflight</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">16</td>
      <td>Integer</td>
      <td>阻止新写请求之前 <code>FirehoseAsyncClient</code> 未完成请求的请求阈值。</td>
    </tr>
    <tr>
      <td><h5>sink.requests.max-buffered</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">10000</td>
      <td>String</td>
      <td>在阻止新的写入请求之前，<code>FirehoseAsyncClient</code> 会设置请求缓冲区阈值。</td>
    </tr>
    <tr>
      <td><h5>sink.flush-buffer.size</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">5242880</td>
      <td>Long</td>
      <td>刷新前， <code>FirehoseAsyncClient</code> 中写入程序缓冲区的阈值（字节）。</td>
    </tr>
    <tr>
      <td><h5>sink.flush-buffer.timeout</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">5000</td>
      <td>Long</td>
      <td>元素在刷新前进入 <code>FirehoseAsyncClient</code> 缓冲区的阈值时间（毫秒）。</td>
    </tr>
    <tr>
      <td><h5>sink.fail-on-error</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>用于重试失败请求的标志。如果设置了任何请求失败，将不会重试，并将使作业失败。</td>
    </tr>
    </tbody>
</table>

## 授权

确保[创建适当的 IAM 策略](https://docs.aws.amazon.com/firehose/latest/dev/controlling-access.html)有权限读写 Kinesis Data Firehose 传输流。

## 身份验证

根据您的部署，您可以选择不同的凭证提供程序以允许访问 Kinesis Data Firehose 。
默认情况下，降使用 AUTO 凭证提供程序。
如果访问 key 和 密钥 key 设置在部署文件中，这种情况将会使用 BASIC 凭证提供程序。

使用 `aws.credentials.provider` 时是一种特殊情况，[AWSCredentialsProvider](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/index.html?com/amazonaws/auth/AWSCredentialsProvider.html) 可以是**可选的**。  
支持的值如下:

- `AUTO` - 使用默认的 AWS 凭据提供程序链，按以下顺序搜索凭据：ENV_VARS、SYS_PROPS、WEB_IDENTITY_TOKEN、PROFILE和EC2/ECS 凭据提供程序。
- `BASIC` - 使用访问 key ID 和密钥 key 作为配置。
- `ENV_VAR` - 使用 `AWS_ACCESS_KEY_ID` 和 `AWS_SECRET_ACCESS_KEY` 作为环境变量.
- `SYS_PROP` - 使用 Java 系统属性：`aws.accessKeyId` 和 `aws.secretKey`。
- `PROFILE` - 使用 AWS 凭据配置文件创建 AWS 凭据。
- `ASSUME_ROLE` - 通过扮演角色创建 AWS 凭据。必须提供担任该角色的凭据。
- `WEB_IDENTITY_TOKEN` - 通过使用 Web 身份令牌承担角色来创建 AWS 凭据。

## 数据类型映射

Kinesis Data Firehose 将记录存储为 Base64 编码的二进制数据对象，因此它没有内部记录结构的概念。  
相反，Kinesis Data Firehose 记录是按格式反序列化和序列化的，例如“avro”、“csv”或“json”。  
要确定 Kinesis Data Firehose 数据表中的消息数据类型，请选择一种带有`format`关键字的合适的 Flink 格式。  
更多详情请参考 [格式]({{< ref "docs/connectors/table/formats/overview" >}}) 页面。  

## 注意

Kinesis Data Firehose SQL 连接器的当前实现仅支持 Kinesis Data Firehose sink，不提供 source 查询的实现。
查询示例:
```sql
SELECT * FROM FirehoseTable;
```
将得到类似如下的错误提示：  
```
Connector firehose can only be used as a sink. It cannot be used as a source.
```
{{< top >}}
