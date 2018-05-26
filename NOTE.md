### 总体架构与代码结构

__Storm 中涉及到的术语__：

1. stream: 被处理的数据
2. spout: 数据源
3. bolt: 封装数据的处理逻辑
4. executor: 工作线程，执行 spout 和 bolt 的业务逻辑
5. worker: 工作进程，一个 JVM 对应一个工作进程，一个工作进程可以包含一个或多个工作线程 executor
6. task: storm 中最小的处理单元，一个 executor 可以包含一个或多个 task，消息的分发都是从一个 task 到另外一个 task 进行的
7. grouping: 消息分发策略，定义 bolt 结点以何种方式接收数据
8. topology: 以消息分组方式连接起来的 spout 和 bolt 节点网络，定义了运算处理的拓扑结构，处理的是不断流动的消息

__Storm 存储在 ZK 上的元数据__：

```text
+ /storm
| ---- + /workerbeats
| ---- | ---- + /<topology-id>
| ---- | ---- | ---- + /node-port
| ---- | ---- | ---- + /node-port
| ---- + /storms
| ---- | ---- + /<topology-id>
| ---- + /assignments
| ---- | ---- + /<topology-id>
| ---- + /supervivors
| ---- | ---- + /<supervivor-id>
| ---- + /errors
| ---- | ---- + /<topology-id>
| ---- | ---- | ---- + /<component-id>
```

- /storm/workerbeats/<topology-id>/node-port

存储由 node 和 port 指定的 worker 的运行状态和一些统计信息，包括 topology-id，当前 worker 上所有 executor 的统计信息（发送的消息数目，接收的消息数目等），当前 workder 的启动时间以及最后一次更新这些信息的时间。

- /storm/storms/<topology-id>

存储 topology 本身的信息，包括名字、启动时间、运行状态、使用的 worker 数目，以及每个组件的并行度设置，其内容在运行期间是不变的。

- /storm/assignments/<topology-id>

存储 nimbus 为每个 topology 分配的任务信息，包括该 topology 在 nimbus 机器本地的存储目录、被分配到的 supervivor 机器到主机名的映射关系、每个 executor 运行在哪个 worker 上及其启动时间等，该节点在运行过程中会被更新。

- /storm/supervivors/<supervivor-id>

存储 supervivor 的运行统计信息，包括最后一次更新的时间、主机名、supervivor-id、已经使用的端口列表、所有的端口列表，以及运行时间等，该节点在运行过程中会被更新。

- /storm/errors/<topology-id>/<topology-id>/e/<sequential-id>

存储运行过程中每个组件上的错误信息，sequential-id 是一个递增序列号，每个组件最多只会保存最近的 10 条错误信息。

__JStorm 相关介绍__

JStorm 集群包含两类节点：主控节点（Nimbus）和工作节点（Supervisor）。其分别对应的角色如下：

1. 主控节点（Nimbus）上运行 Nimbus Daemon。Nimbus 负责接收 Client 提交的 Topology，分发代码，分配任务给工作节点，监控集群中运行任务的状态等工作。Nimbus 作用类似于 Hadoop中 JobTracker。
2. 工作节点（Supervisor）上运行 Supervisor Daemon。Supervisor 通过 subscribe Zookeeper 相关数据监听 Nimbus 分配过来任务，据此启动或停止 Worker 工作进程。每个 Worker 工作进程执行一个 Topology 任务的子集；单个 Topology 的任务由分布在多个工作节点上的 Worker 工作进程协同处理。

Nimbus 和 Supervisor 节点之间的协调工作通过 Zookeeper 实现。此外，Nimbus 和 Supervisor 本身均为无状态进程，支持 Fail Fast；JStorm 集群节点的状态信息或存储在 Zookeeper，或持久化到本地，这意味着即使 Nimbus/Supervisor 宕机，重启后即可继续工作。这个设计使得 JStorm 集群具有非常好的稳定性。

Zookeeper 上存储的状态数据及 Nimbus/Supervisor 本地持久化数据涉及到的地方较多，详细介绍 Nimbus 之前就上述数据的存储结构简要说明如下：

- JStorm 存储在 ZK 上的数据说明

![image](https://github.com/plotor/plotor.github.io/blob/master/images/2018/jstorm_6.png?raw=false)

- Nimbus 本地数据说明

![image](https://github.com/plotor/plotor.github.io/blob/master/images/2018/jstorm_7.png?raw=false)

- Supervisor 本地数据说明

![image](https://github.com/plotor/plotor.github.io/blob/master/images/2018/jstorm_8.png?raw=false)

### JStorm 相对于 Storm 的区别

JStorm 可以看作是 Storm 的 java 增强版本，除了内核用纯 java 实现外，还包括了 thrift、python、facet ui。从架构上看，其本质是一个 __基于 ZK 的分布式调度系统__：

![image](https://github.com/plotor/plotor.github.io/blob/master/images/2018/jstorm_1.png?raw=false)

JStorm 在内核上对 Storm 的改进有：

1. 模型简化
2. 多维度资源调度
3. 网络通信层改造
4. 采样重构
5. worker/task 内部异步化处理
6. classload、HA

__模型简化__ 将 Storm 的三层管理模型简化为两层：

![image](https://github.com/plotor/plotor.github.io/blob/master/images/2018/jstorm_2.png?raw=false)

JStorm 中 task 直接对应了线程概念，而在 Storm 中是 task 只是线程 executor 的一个执行逻辑单元：

__多维度资源调度__ 分为 cpu、memory、net、disk 四个维度，默认情况下：

> - cpu slots = 机器核数 * 2 -1
> - memory slots = 机器物理内存 / 1024M
> - net slots = min(cpu slots, memory slots)

__网络通信层__ 采用了netty + disruptor 替换 zmq + blockingQueue

__采样重构__

1. 定义了滚动时间窗口
2. 优化缓存map性能
3. 增量采样时间以及减少无谓数据

__worker/task 内部异步化__

异步化和回调是流式框架最基本的两大特征，JStorm 在 task 的计算中将 nextTuple 和 ack/fail 的逻辑分离开来，并在 worker 中采用单独线程负责流入、流出数据的反序列化及序列化工作。

有关 JStorm 实现的几个关键流程，有兴趣的可以参考源码

1.Nimbus 的启动

![image](https://github.com/plotor/plotor.github.io/blob/master/images/2018/jstorm_3.png?raw=false)

2.Supervisor 的启动

![image](https://github.com/plotor/plotor.github.io/blob/master/images/2018/jstorm_4.png?raw=false)

3.Worker 内部结构

![image](https://github.com/plotor/plotor.github.io/blob/master/images/2018/jstorm_5.png?raw=false)

Worker 的启动需要完成以下几件事：

> 1. 读取配置文件，启动进程
> 2. 初始化 tuple 接收队列和发送队列
> 3. 打开端口，启动 rpc 服务
> 4. 创建 context 结构，<component, <stream, output_field>>
> 5. 触发各种 timer,refresh/reconnection/heartbeat...

Task 的工作包括：

> 1. 创建内部队列，bind connection
> 2. 反射 component 拿到 taskObj，创建具体的 spout/bolt executor
> 3. 反序列化 tuple 数据，执行处理逻辑
> 4. 做 stats，heartbeat 等

JStorm 在数据的完整性和准确性上分别依赖了 acker 和事务机制：

Acker 本质是独立的 bolt，input 是 fieldGrouping，output 是 directGrouping；每个 bolt 有两个 output stream(ACKER_ACK_STREAM_ID/ACKER_ACK_FAIL_STREAM_ID);每个 spout 有一个 output stream(ACKER_INIT_STREAM_ID),以及两个 input stream(ACKER_ACK_STREAM_ID/ACKER_ACK_FAIL_STREAM_ID)

- Spout

> - 发送给 acker 的 `value <rootid, xor(target_task_list)>`
> - 发送下一级 bolt 的 `value <rootid, 目标taskid>`

- Bolt

> - 下一级 bolt 需要 ack 发送给下一级 bolt 为 `<rootid, 新uuid)>` 发送给 acker 的 value为 `<rootid, xor(新uuid, $(接收值))>`
> - 下一级 bolt 不需要 ack 发送给下一级 bolt 为空发送给 acker 为 `<rootid, $(接收值)>`

__事务__：批处理 + 全局唯一递增 id + 两阶段提交

在发送 tuple 的时候带上 tid 来保证“只有一次”的原语，下游逻辑根据 tid 是否 next tid 来判断是否需要处理。为了提高效率，会将多个 tuple 组装成一批赋予一个 tid，并用 pipeline 方式执行 processing 和 commit 阶段，其中 processing 可以并发执行，而 commit 具有严格的强顺序性。接口 coordinator，commitor 中做了状态管理、事务协调、错误检查等工作。

另外一个用得最多的高级特性就是 __trident__，它对 bolt 进行了封装，提供了如 joins、aggregations、grouping、filters、function 等多种高级数据处理能力。
