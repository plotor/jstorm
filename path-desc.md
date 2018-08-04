## 路径存储内容说明

### ZK 数据说明

```
+ ${zk_root_dir}
| ---- + topology : 保存正在运行的 ${topology_id}
| ---- | ---- + ${topology_id} : 拓扑信息（名称、开始运行时间、运行状态）

| ---- + supervisors : 保存所有 supervisor 的心跳信息
| ---- | ---- + ${supervivor_id} : 心跳信息（心跳时间、主机名称、所有 worker 的端口号、运行时间等）

| ---- + assignments : 拓扑任务分配信息
| ---- | ---- + ${topology_id} : 任务分配信息（对应 nimbus 上的代码目录、所有 task 的启动时间、每个 task 与节点的映射）

| ---- + assignments_bak : 

| ---- + tasks : 所有的 task
| ---- | ---- + ${topology_id} : 记录当前 topology 对应的所有 task
| ---- | ---- | ---- + ${task_id} : task 对应的 component_id（bolt_id 或 spout_id）

| ---- + taskbeats : 所有 task 的心跳信息
| ---- | ---- + ${topology_id} : 记录当前 topology 对应的所有 task 的心跳信息
| ---- | ---- | ---- + ${task_id} : 心跳信息（心跳时间、运行时长、统计信息）

| ---- + taskerrors : 所有 task 的错误信息
| ---- | ---- + ${topology_id} : 记录当前 topology 对应的所有 task 的错误信息
| ---- | ---- | ---- + ${task_id} : 错误信息

| ---- + metrics

| ---- + blobstore

| ---- + gray_upgrade
```

### Nimbus 本地数据说明

```
+ ${local_dir}
| ---- + nimbus
| ---- | ---- + inbox : 存放客户端上传的 jar 包
| ---- | ---- | ---- + stormjar-{uuid}.jar : 对应一个具体的 jar 包
| ---- | ---- + stormdist
| ---- | ---- | ---- + ${topology_id}
| ---- | ---- | ---- | ---- + stormjar.jar : 包含当前拓扑所有代码的 jar 包（从 inbox 那复制过来的）
| ---- | ---- | ---- | ---- + stormcode.ser : 当前拓扑对象的序列化
| ---- | ---- | ---- | ---- + stormconf.ser : 当前拓扑的配置信息
```

### Supervisor 本地数据说明

```
+ ${local_dir}
| ---- + supervisor
| ---- | ---- + stormdist
| ---- | ---- | ---- + ${topology_id}
| ---- | ---- | ---- | ---- + resources : 拓扑 jar 包 resources 目录下面的所有文件
| ---- | ---- | ---- | ---- + stormjar.jar : 包含当前拓扑所有代码的 jar 包（从 inbox 那复制过来的）
| ---- | ---- | ---- | ---- + stormcode.ser : 当前拓扑对象的序列化
| ---- | ---- | ---- | ---- + stormconf.ser : 当前拓扑的配置信息
| ---- | ---- + localstate : local state 信息
| ---- | ---- + tmp : 临时目录，从 nimbus 下载的文件的临时存储目录，简单处理之后复制到 stormdist/${topology_id}
| ---- | ---- | ---- + ${uuid}
| ---- | ---- | ---- | ---- + stormjar.jar : 从 Nimbus 上下载下来的 jar 包
| ---- | ---- | ---- + ${topology_id}
| ---- | ---- | ---- | ---- + stormjar.jar : 包含当前拓扑所有代码的 jar 包（从 inbox 那复制过来的）
| ---- | ---- | ---- | ---- + stormcode.ser : 当前拓扑对象的序列化
| ---- | ---- | ---- | ---- + stormconf.ser : 当前拓扑的配置信息

| ---- + workers
| ---- | ---- + ${worker_id}
| ---- | ---- | ---- + pids : woker 启动的多个子进程
| ---- | ---- | ---- | ---- + ${pid} : 运行当前 worker 的 JVM 的进程 ID
| ---- | ---- | ---- + heartbeats : 心跳信息
| ---- | ---- | ---- | ---- + ${worker_id} : worker 心跳信息（心跳时间、worker 的进程 ID）
```
