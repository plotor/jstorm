## 路径存储内容说明

### ZK 数据说明

```text
+ ${zk_root_dir}
| ---- + topology: 记录集群中所有正在运行的 topology 数据
| ---- | ---- + ${topology_id}: 指定 topology 的相关信息（名称、开始运行时间、运行状态等）

| ---- + supervisors: 记录集群中所有 supervisor 节点的心跳信息
| ---- | ---- + ${supervivor_id}: 指定 supervisor 的心跳信息（心跳时间、主机名称、所有 worker 的端口号、运行时间等）

| ---- + assignments: 记录提交给集群的 topology 任务分配信息
| ---- | ---- + ${topology_id}: 指定 topology 的任务分配信息（对应 nimbus 上的代码目录、所有 task 的启动时间、每个 task 与节点和端口的映射关系等）

| ---- + assignments_bak: 记录提交给集群的 topology 任务分配信息的备份

| ---- + tasks: 记录集群中所有 topology 的 task 信息
| ---- | ---- + ${topology_id}: 指定 topology 的所有 task 信息
| ---- | ---- | ---- + ${task_id}: 指定 task 所属的组件 ID 和类型（spout/bolt）

| ---- + taskbeats: 记录集群中所有 task 的心跳信息
| ---- | ---- + ${topology_id}: 记录指定 topology 下所有 task 的心跳信息、topologyId，以及 topologyMasterId 等
| ---- | ---- | ---- + ${task_id}: 指定 task 的心跳信息（最近一次心跳时间、运行时长、统计信息等）

| ---- + taskerrors: 记录集群中所有 topology 的 task 运行错误信息
| ---- | ---- + ${topology_id}: 指定 topology 下所有 task 的运行错误信息
| ---- | ---- | ---- + ${task_id}: 指定 task 的运行错误信息

| ---- + metrics: 记录集群中所有 topology 的 metricsId

| ---- + blobstore: 记录集群对应的 blobstore 信息，用于协调数据一致性

| ---- + gray_upgrade: 记录灰度发布中的 topologyId
```

### Nimbus 本地数据说明

```
+ ${nimbus_local_dir}
| ---- + nimbus
| ---- | ---- + inbox: 存放客户端上传的 jar 包
| ---- | ---- | ---- + stormjar-{uuid}.jar: 对应一个具体的 jar 包
| ---- | ---- + stormdist
| ---- | ---- | ---- + ${topology_id}
| ---- | ---- | ---- | ---- + stormjar.jar: 包含当前拓扑所有代码的 jar 包（从 inbox 那复制过来的）
| ---- | ---- | ---- | ---- + stormcode.ser: 当前拓扑对象的序列化文件
| ---- | ---- | ---- | ---- + stormconf.ser: 当前拓扑的配置信息文件
```

### Supervisor 本地数据说明

```
+ ${supervisor_local_dir}
| ---- + supervisor
| ---- | ---- + stormdist
| ---- | ---- | ---- + ${topology_id}
| ---- | ---- | ---- | ---- + resources: 指定 topology 程序包 resources 目录下面的所有文件
| ---- | ---- | ---- | ---- + stormjar.jar: 包含指定 topology 所有代码的 jar 文件
| ---- | ---- | ---- | ---- + stormcode.ser: 包含指定 topology 对象的序列化文件
| ---- | ---- | ---- | ---- + stormconf.ser: 包含指定 topology 的配置信息文件
| ---- | ---- + localstate: 本地状态信息
| ---- | ---- + tmp: 临时目录，从 nimbus 下载的文件的临时存储目录，简单处理之后复制到 stormdist/${topology_id}
| ---- | ---- | ---- + ${uuid}
| ---- | ---- | ---- | ---- + stormjar.jar: 从 nimbus 节点下载下来的 jar 文件
| ---- | ---- | ---- + ${topology_id}
| ---- | ---- | ---- | ---- + stormjar.jar: 包含指定 topology 所有代码的 jar 文件（从 inbox 目录复制过来）
| ---- | ---- | ---- | ---- + stormcode.ser: 包含指定 topology 对象的序列化文件
| ---- | ---- | ---- | ---- + stormconf.ser: 包含指定 topology 的配置信息文件

| ---- + workers
| ---- | ---- + ${worker_id}
| ---- | ---- | ---- + pids
| ---- | ---- | ---- | ---- + ${pid}: 指定 worker 进程 ID
| ---- | ---- | ---- + heartbeats
| ---- | ---- | ---- | ---- + ${worker_id}: 指定 worker 心跳信息（心跳时间、worker 的进程 ID）
```
