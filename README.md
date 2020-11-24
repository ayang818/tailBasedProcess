# Trace-filter
> 阿里云第一届云原生编程挑战赛(分布式统计和过滤的链路追踪)

> 排名：116/4404

参照赛题方给的Demo思路做的一个extend版本，在正式赛的时候排名50左右，但在最终赛由于换了一组数据并且没有给出运行日志，没有找到错误，也就没有跑出成绩。但是其中部分思路还是比较好的。
包括ring buffer工作时的同步策略，backend的生产消费模型等。

赛题详情

## 引言

云原生环境是错综复杂，而观察性就是云原生的眼睛。云原生的可观察性面临比较多的挑战，比如高性能，低资源，高质量数据。本赛题主要是考察链路追踪中的Tail-based Sampling。

## 问题背景

为了应对各种复杂的业务，系统架构也从单机大型软件演化成微服务架构。微服务构建在不同的软件集上，这些软件模块可能是由不同团队开发的，可能使用不同的编程语言来实现，还可能发布在多台服务器上。因此，如果一个服务出现问题，可能导致几十个服务都出现异常。

分布式追踪系统可以记录请求范围内的信息，包括用户在页面的一次点击发送请求，这个请求的所有处理过程，比如经过多少个服务，在哪些机器上执行，每个服务的耗时和异常情况。可参考 [链路追踪的概念](https://help.aliyun.com/document_detail/90498.html)

采集链路数据过程中，采集的数据越多，消耗的成本就越多。为了降低成本，目前普遍的做法是对数据进行采样。请求是否采样都是从的头节点决定并通过跟踪上下文透传到所有节点(head-based sampling)。目前业界普遍的采样都是按照这个方式，比如固定比例采样(Probabilistic Sampling)，蓄水池采样(Rate Limiting Sampling)，混合采样(Adaptive Sample)。这样的采样可以保证整个调用链的完整性。但是这样采样也带来两个问题，一 有些异常和慢请求因为采样的原因没有被采集到，而这些链路数据对业务很重要。二 99%的请求都是正常的，而这些数据对问题排查并不重要，也就是说大量的成本花在并不重要的数据上。

本题目是另外一种采样方式(tail-based Sampling)，只要请求的链路追踪数据中任何节点出现重要数据特征（错慢请求），这个请求的所有链路数据都采集。目前开源的链路追踪产品都没有实现tail-based Sampling，主要的挑战是：任何节点出现符合采样条件的链路数据，那就需要把这个请求的所有链路数据采集。即使其他链路数据在这条链路节点数据之前还是之后产生，即使其他链路数据在分布式系统的成百上千台机器上产生。

## 一 整体流程

用户需要实现两个程序，一个是数量流（橙色标记）的处理程序，该机器可以获取数据源的http地址，拉取数据后进行处理，一个是后端程序（蓝色标记），和客户端数据流处理程序通信，将最终数据结果在后端引擎机器上计算。
![enter image description here](https://tianchi-public.oss-cn-hangzhou.aliyuncs.com/public/files/forum/158937474327652321589374742874.png)
**流程图**

### 1.1、数据源

数据来源：采集自分布式系统中的多个节点上的调用链数据，每个节点一份数据文件。数据格式进行了简化，每行数据(即一个span)包含如下信息：

traceId | startTime | spanId | parentSpanId | duration | serviceName | spanName | host | tags

具体各字段的：

- traceId：全局唯一的Id，用作整个链路的唯一标识与组装
- startTime：调用的开始时间
- spanId: 调用链中某条数据(span)的id
- parentSpanId: 调用链中某条数据(span)的父亲id，头节点的span的parantSpanId为0
- duration：调用耗时
- serviceName：调用的服务名
- spanName：调用的埋点名
- host：机器标识，比如ip，机器名
- tags: 链路信息中tag信息，存在多个tag的key和value信息。格式为key1=val1&key2=val2&key3=val3 比如 http.status_code=200&error=1

数据总量要足够大，暂定 4G。

文件里面有很个调用链路（很多个traceId）。每个调用链路内有很多个span（相同的traceId，不同的spanId）

例如文件1有

d614959183521b4b|1587457762873000|d614959183521b4b|0|311601|order|getOrder|192.168.1.3|http.status_code=200

d614959183521b4b|1587457762877000|69a3876d5c352191|d614959183521b4b|305265|Item|getItem|192.168.1.3|http.status_code=200

文件2有

d614959183521b4b|1587457763183000|dffcd4177c315535|d614959183521b4b|980|Loginc|getLogisticy|192.168.1.2|http.status_code=200

d614959183521b4b|1587457762878000|52637ab771da6ae6|d614959183521b4b|304284|Loginc|getAddress|192.168.1.2|http.status_code=200

### 1.2、数据流

官方提供两个程序（Docker 容器），以流的形式输出数据，参赛选手无法直接读取到数据文件。这样做的目的是让该比赛是一个面向流处理的场景，而不是批处理的场景。

在实际情况中，每个数据文件在时间上是顺序的，但是一个调用链的一个环节出现在哪个节点的文件中是不确定的。为了模拟这种跨节点的数据乱序的特性，我们生成的数据文件其实是一个大文件，该文件穿插了节点里面的若干个文件。因为提供的数据流的特性是：局部有序，全局无序。

为了方便选手解题，相同traceId的第一条数据（span）到最后一条数据不会超过2万行。方便大家做短暂缓存的流式数据处理。真实场景中会根据时间窗口的方式来处理，超时数据额外处理。

数据流处理程序之间不可以直接通信。因为数据流处理程序数量增加时，程序之间的通信出现几何级别倍增。在真实环境下，数据流处理程序可能有上百个，而且是部署在客户端，节点间通信的成本很大。处理流处理程序既要承担数据过滤职责还要维护多个节点之间通信，架构会过于复杂和脆弱。

### 1.3、后端引擎

上述数据流的两个程序可以和服务端通信，数据流处理程序不可以全量上报到后端数据引擎。只可以上报符合条件的数据和traceId。 在实际业务场景中，全量上报数据会导致网络的负载过大，把网卡打满，需要在数据流处理时进行过滤后上报存储。

## 二 、要求

找到所有tags中存在 http.status_code 不为 200，或者 error 等于1的调用链路。输出数据按照 traceId 分组，并按照startTime升序排序。记录每个 traceId 分组下的全部数据，最终输出每个traceId下所有数据的CheckSum(Md5值）。

注意：一条链路的数据，可能会出现两个数据流中，例如span在数据流1中，parentSpan在数据流2中。 当数据流1中某个span出现符合要求时，那对应的调用链(相同的traceId)的其他所有span都需要保存，即使其他span出现在数据流2中。

## 三、验证

官方提供一个验证程序，用来将选手生成的结果同已知结果进行对比，计算 F1 Score；同时记录整个跑分从开始到输出结果的时间 time，最后的比赛得分: F1/time。即 F1 值越大越好，耗时越小越好。

## 四、评测方式

用户提供一个docker image，评测程序会启动三个docker 容器，其中两个2核4G的容器会配置环境变量"SERVER_PORT"为8000和8001端口，对应的是流程图中的数据流程序(橙色), 另外一个1核2G的容器会配置环境变量"SERVER_PORT"为8002，对应的是流程图中的后端引擎（蓝色）。评测将分为三个阶段。

### 4.1 开始阶段

评测程序会调用用户docker容器的接口1，检查到某个docker的http接口已经返回状态为200后开始计时。当用户三个docker都返回200后，将会调用接口2

- 接口1：
  HTTP 1.1 Get localhost:{8000,8001,8002}/ready：一个状态接口，启动时评分程序会对探测，返回 HTTP 200 即表示选手的程序已经就绪，可以执行后续操作
- 接口2：
  HTTP 1.1 Get localhost:{8000,8001,8002}/setParameter：一个接收数据源的接口，格式为 {"port":"$port"} ，例如 {"port":"8080"} 返回 HTTP 200

### 4.2 运行计算阶段

用户程序收到数据源的端口后，可以调用http接口拉取数据。可以拉取到两份数据， http接口的格式为：HTTP 1.1 localhost:$port/trace1.data, localhost:$port/trace2.data

### 4.3 运行完成阶段

用户程序计算完数据后，需要将运行结果进行上报，该接口的具体url会在接口2中，用户运行完成后一次性返回所有traceID和对应的checksum（同一个traceId下的所有span按照startTime升序排列后生成的md5值），上报的接口格式为：

HTTP 1.1 POST "[http://localhost](http://localhost/):${port}/api/finished"：参数为 result={"$traceId1":"$checksum1","$traceId2":"$checksum2"}. curl 模拟请求为： curl -d 'result={"d614959183521b4b":"d8de6a65bd9f0b80f935b44dc6dff8d5"}' "http://localhost:8080/api/finished"

## 五、本地调试

为了方便选手进行开发，提供了测试数据和本地运行方式

### 5.1 程序方式

测试数据分两份,一份是链路数据，分两个文件，[trace1.data (400M)](https://tianchi-competition.oss-cn-hangzhou.aliyuncs.com/231790/trace1_data.tar.gz?OSSAccessKeyId=LTAI4G7mrxYb7QrcXkTr3zzt&Expires=1593247540&Signature=YZLVJiJiJOga/KY4Z8dKVPyGJAQ=)和[trace2.data(400m)](https://tianchi-competition.oss-cn-hangzhou.aliyuncs.com/231790/trace2_data.tar.gz?OSSAccessKeyId=LTAI4G7mrxYb7QrcXkTr3zzt&Expires=1593247613&Signature=Z7PZrVG1UXnXdbO08bPv3hBCfyA=). 一份是[checksum](https://tianchi-competition.oss-cn-hangzhou.aliyuncs.com/231790/checkSum.data?OSSAccessKeyId=LTAI4G7mrxYb7QrcXkTr3zzt&Expires=1593244605&Signature=dCeICVXT9RrnjCMNEq4Gpduw1XE=)数据，用来校验结果。用户本地调试时可以运行程序分别访问链路数据文件（可以放本地文件，或者文件服务器中，比如nginx）。
评测程序，本地调试是可以运行java版本的[评测程序](https://tianchi-competition.oss-cn-hangzhou.aliyuncs.com/231790/scoring-1.0-SNAPSHOT.jar?OSSAccessKeyId=LTAI4G7mrxYb7QrcXkTr3zzt&Expires=1597352721&Signature=TeyHTwtUbLP2g65xq3w%2FQl6Qn0I%3D)，启动评测程序前，需要数据处理程序和后台处理程序都运行起来（端口8000，8001，8002已经开启）。
以demo程序为例，先启动处理程序和后台处理程序。

```
java -Dserver.port=8000 -jar /path/to/userjar/ &
java -Dserver.port=8001 -jar /path/to/userjar/ &
java -Dserver.port=8002 -jar /path/to/userjar/ &
```

再启动评测程序，

```
java -Dserver.port=9000 -DcheckSumPath=/tmp/checkSum.data -jar /path/to/scoring-1.0-SNAPSHOT.jar
```

注意：评测程序没有带上测试数据，本地运行demo时，需要修改拉取数据的方式。

为了更接近最终评测的数据，提供了一份单个文件为4G左右的测试数据：
[trace1.data(4G)](https://tianchi-competition.oss-cn-hangzhou.aliyuncs.com/231790/trace1.big.tgz?OSSAccessKeyId=LTAI4G7mrxYb7QrcXkTr3zzt&Expires=1594211594&Signature=tcdqO9luRyF8E4bgcnXpGBm%2BTRI%3D) ，[trace2.data(4G)](https://tianchi-competition.oss-cn-hangzhou.aliyuncs.com/231790/trace2.big.tgz?OSSAccessKeyId=LTAI4G7mrxYb7QrcXkTr3zzt&Expires=1594211862&Signature=%2BR3QRLVnkd9hS0u1FhnCBhXa1wc%3D) . [checksum](https://tianchi-competition.oss-cn-hangzhou.aliyuncs.com/231790/checkSum.big.tgz?OSSAccessKeyId=LTAI4G7mrxYb7QrcXkTr3zzt&Expires=1594211937&Signature=AvSSZF%2BTn2YgpGR7cBX77DJlXEI%3D)

### 5.2 docker 方式测试

- 打包的docker image，
  将用户程序打包成docker image, 例如demo打包docker image，DockerFile 如下

```
#基础镜像
FROM centos:centos7

#将jdk8包放入/usr/local/src并自动解压，jdk8.tar.gz 需要到oracle官方下载，注意解压后的java版本号
ADD jdk8.tar.gz /usr/local/src
COPY tailbaseSampling-1.0-SNAPSHOT.jar /usr/local/src
WORKDIR /usr/local/src
COPY start.sh /usr/local/src
RUN chmod +x /usr/local/src/start.sh
ENTRYPOINT ["/bin/bash", "/usr/local/src/start.sh"]
```

对应的启动脚本

```
if   [  $SERVER_PORT  ];
then
   /usr/local/src/jdk1.8.0_251/bin/java -Dserver.port=$SERVER_PORT -jar /usr/local/src/tailbaseSampling-1.0-SNAPSHOT.jar &
else
   /usr/local/src/jdk1.8.0_251/bin/java -Dserver.port=8000 -jar /usr/local/src/tailbaseSampling-1.0-SNAPSHOT.jar &
fi
tail -f /usr/local/src/start.sh
```

- 启动运行docker image
  通过host网络方式启动docker image

```
  docker pull registry.cn-hangzhou.aliyuncs.com/cloud_native_match/tail-base-sampling:0.2
  docker run --rm -it  --net host -e "SERVER_PORT=8000" --name "clientprocess1" -d tail-base-sampling:0.2
  docker run --rm -it  --net host -e "SERVER_PORT=8001" --name "clientprocess2" -d tail-base-sampling:0.2
  docker run --rm -it  --net host -e "SERVER_PORT=8002" --name "backendprocess" -d tail-base-sampling:0.2
```

- 启动评测程序

```
docker pull registry.cn-hangzhou.aliyuncs.com/cloud_native_match/scoring:0.1
docker run --rm --net host -e "SERVER_PORT=8081" --name scoring -d scoring:0.1
```

## 六 赛题DEMO

赛题用java做了一个demo，说明评测程序和用户编写的程序的接口交互过程。请点击链接：(https://code.aliyun.com/middleware-contest-2020/tail-based-sampling)

## 七 提交评测

- 构建docker
  可以参考5.2 docker 方式测试来构建docker image。docker 的镜像服务建议用 ([阿里云容器镜像服务](https://www.aliyun.com/product/acr)),个人用户有免费额度，够这次大赛使用。
  开通后进入镜像仓库: [https://cr.console.aliyun.com](https://cr.console.aliyun.com/)
  [天池Docker相关的手把手教程](https://tianchi.aliyun.com/competition/entrance/231759/tab/174)
- 提交docker image
  提交image的地址需要带上版本号，用户名，密码。如下图
  ![enter image description here](https://tianchi-public.oss-cn-hangzhou.aliyuncs.com/public/files/forum/159099936663749241590999366042.png)
  注意：创建docker运行有20分钟的超时设定。也就是说20分钟没有运行完，将会停止评测。
  如果返回的评测日志中包含关键字“trying and failing to pull image", 表示拉取不到image。需要检查image地址，用户名，密码。

# 八 发布变更

## 7月3号 更新数据，有以下变动

- 添加了长tag，比如sql=select*from * join ,exception=NullPointException, stack=com.adf. 一个tags的长度可以达到3000多个字节。
  调整原因：在真实环境下，一个请求进入的微服务不同，产生的数据不一样，有些只是中转那数据会简单，那有大量数据库调用或者缓存处理，那span的数据会更复杂一些，相应的数据会更多。

由于长 tag 的加入，会导致trace1.data 和 data2.data的数据大小会相差1个多G。 也就是说同一个trace下的两个span在两个文件的行数相差十行，但是在文件的位置可能相差1个G。 用range切分的同学需要注意。
[trace1.data](https://tianchi-competition.oss-cn-hangzhou.aliyuncs.com/231790/trace1_final.tgz?OSSAccessKeyId=LTAI4G7mrxYb7QrcXkTr3zzt&Expires=1594408476&Signature=qEMERAZZPkLCqAJf%2Fhh8ybWmYPs%3D) ，[trace2.data](https://tianchi-competition.oss-cn-hangzhou.aliyuncs.com/231790/trace2_final.tgz?OSSAccessKeyId=LTAI4G7mrxYb7QrcXkTr3zzt&Expires=1594408510&Signature=QC2deh7cTeQNUXhu8cwgScXFq6U%3D) . [checksum](https://tianchi-competition.oss-cn-hangzhou.aliyuncs.com/231790/checkSum_final.tgz?OSSAccessKeyId=LTAI4G7mrxYb7QrcXkTr3zzt&Expires=1594408557&Signature=CcmuIq7TacwOmqblpQ0sj7tjKfc%3D)

## 6月23号 更新数据，有以下变动

- error=1 和 http.status_code!= 200不再位于尾部
  调整原因：部分选手只做尾部检查，不做tags的整体过滤，和实际场景不符合
- 开始时间优先取访问trace1/2.data的时间，而不是发送parameter的时间。
  调整原因：防止用户用端口探测方式，预先拉取数据，提前计算。
- 单个文件的数据量从原来的4.8G 提升到6.4G [trace1.data(6G)](https://tianchi-competition.oss-cn-hangzhou.aliyuncs.com/231790/trace1_6g.tar.gz?OSSAccessKeyId=LTAI4G7mrxYb7QrcXkTr3zzt&Expires=1596801848&Signature=QxosyBaMKeLOV6Uy4rjeT4zcGIw%3D) [trace2.data(6G)](https://tianchi-competition.oss-cn-hangzhou.aliyuncs.com/231790/trace2_6g.tar.gz?OSSAccessKeyId=LTAI4G7mrxYb7QrcXkTr3zzt&Expires=1596801893&Signature=YxMsWh%2F0yLCo5rkVMrDA71EF3Ww%3D) [checksum.data](https://tianchi-competition.oss-cn-hangzhou.aliyuncs.com/231790/checkSum_6g_.data?OSSAccessKeyId=LTAI4G7mrxYb7QrcXkTr3zzt&Expires=1596802813&Signature=ZVbneqbAked%2BeOWhTJIm3UgArXI%3D)
  调整原因：加大数据量，更充分释放计算，让运气成分更小一些。
- 调整f1计算，数据全对和数据错一个的分数差距拉大。
  调整原因：数据质量比速度更重要。只要数据准确的前提下，处理越快越好。如果链路数据出现偏差，会丢失用户的信任，再快也是徒劳。

关于群里讨论比较多的rang拉取方式。 可以多线程rang方式拉取，但是只能拉取对应的数据。比如说8000端口的机器不可以拉取trace2.data，只能拉取trace1.data。 backend不可以取拉取数据。