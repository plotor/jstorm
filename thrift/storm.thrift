#!/usr/local/bin/thrift --gen java:beans,nocamel,hashcode --gen py:utf8strings
namespace java backtype.storm.generated

union JavaObjectArg {
  1: i32 int_arg;
  2: i64 long_arg;
  3: string string_arg;
  4: bool bool_arg;
  5: binary binary_arg;
  6: double double_arg;
}

struct JavaObject {
  1: required string full_class_name;
  2: required list<JavaObjectArg> args_list;
}

struct NullStruct {
  
}

struct GlobalStreamId {
  1: required string componentId; // 当前组件输入流来源组件 ID
  2: required string streamId; // 当前组件所输出的特定的流
  #Going to need to add an enum for the stream type (NORMAL or FAILURE)
}

/**
 * 消息分组方式
 **/
union Grouping {
  1: list<string> fields; //empty list means global grouping
  2: NullStruct shuffle; // tuple is sent to random task
  3: NullStruct all; // tuple is sent to every task
  4: NullStruct none; // tuple is sent to a single task (storm's choice) -> allows storm to optimize the topology by bundling tasks into a single process
  5: NullStruct direct; // this bolt expects the source bolt to send tuples directly to it
  6: JavaObject custom_object;
  7: binary custom_serialized;
  8: NullStruct local_or_shuffle; // prefer sending to tasks in the same worker process, otherwise shuffle
  9: NullStruct localFirst; //  local worker shuffle > local node shuffle > other node shuffle
}

/**
 * 流信息
 **/
struct StreamInfo {

  // 输出字段列表
  1: required list<string> output_fields;

  // 是否为 direct 流
  2: required bool direct;

}

/**
 * 用于与非 java 语言交互，通过标准输入输出来交换数据
 **/
struct ShellComponent {
  // should change this to 1: required list<string> execution_command;
  1: string execution_command;
  2: string script;
}

/**
 * 串行化后的 java 对象 or ShellComponent 对象 or java 对象
 **/
union ComponentObject { // 联合体，三个属性同时只有一个被赋值

  // 记录序列化后的 java 对象
  1: binary serialized_java;

  // ShellComponent 对象
  2: ShellComponent shell;

  // java 对象
  3: JavaObject java_object;

}

/**
 * 用来表示 Topology 的基础对象
 **/
struct ComponentCommon {

  // 表示该组件将从哪些 GlobalStreamId 以何种分组方式接收数据
  1: required map<GlobalStreamId, Grouping> inputs;

  // 表示该组件要输出的所有流
  2: required map<string, StreamInfo> streams; // key is stream id

  // 组件并行度，即多少个线程，这些线程可能分布在不同的机器以及进程空间中
  3: optional i32 parallelism_hint;

  /**
   * 组件相关的配置:
   * topology.debug: false  // 如果为 true 则会打印所有发送出去的消息
   * topology.max.task.parallelism: null // 任务的最大并行度，通常用于测试
   * topology.max.spout.pending: null // 表示最多允许多少没有被 ack/fail 的消息在系统中运行
   * topology.kryo.register // kryo 序列化注册列表
   **/
  4: optional string json_conf;

}

struct SpoutSpec {

  // 实现具体 spout 逻辑的对象
  1: required ComponentObject spout_object;

  // 描述其输入输出的 common 对象
  2: required ComponentCommon common;
  // can force a spout to be non-distributed by overriding the component configuration
  // and setting TOPOLOGY_MAX_TASK_PARALLELISM to 1
}

// BoltSpec
struct Bolt {
  // 实现具体 bolt 逻辑的序列化对象
  1: required ComponentObject bolt_object;

  // 描述其输入输出的 ComponentCommon 对象
  2: required ComponentCommon common;
}

// not implemented yet
// this will eventually be the basis for subscription implementation in storm
struct StateSpoutSpec {
  1: required ComponentObject state_spout_object;
  2: required ComponentCommon common;
}

/**
 * 描述 Topology 的组成
 **/
struct StormTopology {
  // ids must be unique across maps
  // #workers to use is in conf
  1: required map<string, SpoutSpec> spouts; // topology 中的 spout 集合
  2: required map<string, Bolt> bolts; // topology 中的 bolt 集合
  3: required map<string, StateSpoutSpec> state_spouts;
}

exception AlreadyAliveException {
  1: required string msg;
}

exception NotAliveException {
  1: required string msg;
}

exception AuthorizationException {
  1: required string msg;
}

exception InvalidTopologyException {
  1: required string msg;
}

exception TopologyAssignException {
  1: required string msg;
}

exception KeyNotFoundException {
  1: required string msg;
}

exception KeyAlreadyExistsException {
  1: required string msg;
}

/**
 * 描述了由用户提交的 Topology 的基本情况，主要供 Nimbus 使用
 **/
struct TopologySummary {
  1: required string id;
  2: required string name;
  3: required string status;
  4: required i32 uptimeSecs; // 运行时长
  5: required i32 numTasks;
  6: required i32 numWorkers;
  7: optional string errorInfo;
}

/**
 * 描述了每一个 Supervisor 的基本信息
 * Supervisor 代表机器，一个 Supervisor 上可以启动多个 Worker （通常为 4 个），每个 Worker 上可以启动多个 Executor
 * 主要供 Nimbus 使用
 **/
struct SupervisorSummary {
  1: required string host; // 主机名
  2: required string supervisorId; // ID
  3: required i32 uptimeSecs; // 启动时间
  4: required i32 numWorkers; // 可以使用的端口数目
  5: required i32 numUsedWorkers; // 已经使用的端口数目
  6: optional string version;
  7: optional string buildTs;
  8: optional i32 port;
  9: optional string errorMessage;
}

struct NimbusStat {
  1: required string host;
  2: required string uptimeSecs;
}

struct NimbusSummary {
  1: required NimbusStat nimbusMaster;
  2: required list<NimbusStat> nimbusSlaves;
  3: required i32 supervisorNum;
  4: required i32 totalPortNum;
  5: required i32 usedPortNum;
  6: required i32 freePortNum;
  7: required string version;
}

/**
 * 集群中所包含的 Supervisor 的数目及其基本信息，以及正在集群上运行的 Topology 基本信息
 **/
struct ClusterSummary {
  1: required NimbusSummary nimbus;
  2: required list<SupervisorSummary> supervisors;
  3: required list<TopologySummary> topologies;
}

struct TaskComponent {
  1: required i32 taskId
  2: required string component;
}

struct WorkerSummary {
  1: required i32 port;
  2: required i32 uptime;
  3: required string topology;
  4: required list<TaskComponent> tasks
}

struct MetricWindow {
  // map<second, double>, 0 means all time
  1: required map<i32, double> metricWindow;
}

// a union type for counter, gauge, meter, histogram and timer
struct MetricSnapshot {
  1: required i64 metricId;
  2: required i64 ts;
  3: required i32 metricType;
  4: optional i64 longValue;
  5: optional double doubleValue;
  6: optional double m1;
  7: optional double m5;
  8: optional double m15;
  9: optional double mean;
  10: optional i64 min;
  11: optional i64 max;
  12: optional double p50;
  13: optional double p75;
  14: optional double p95;
  15: optional double p98;
  16: optional double p99;
  17: optional double p999;
  18: optional double stddev;
  19: optional binary points;
  20: optional i32 pointSize;
}

struct MetricInfo {
  // map<metricName, map<window, snapshot>>
  1: optional map<string, map<i32, MetricSnapshot>> metrics;
}

struct SupervisorWorkers {
  1: required SupervisorSummary supervisor;
  2: required list<WorkerSummary> workers;
  3: required map<string,MetricInfo> workerMetric;
}

struct ErrorInfo {
  1: required string error;
  2: required i32 errorTimeSecs;
  3: required string errorLevel;
  4: required i32 errorCode;
}

struct ComponentSummary {
  1: required string name;
  2: required i32 parallel;
  3: required string type;
  4: required list<i32> taskIds;
  5: optional list<ErrorInfo>  errors;
}

struct TaskSummary {
  1: required i32 taskId;
  2: required i32 uptime;
  3: required string status;
  4: required string host;
  5: required i32 port;
  6: optional list<ErrorInfo>  errors;
}

struct TopologyMetric {
  1: required MetricInfo topologyMetric;
  2: required MetricInfo componentMetric;
  3: required MetricInfo workerMetric;
  4: required MetricInfo taskMetric;
  5: required MetricInfo streamMetric;
  6: required MetricInfo nettyMetric;
  7: optional MetricInfo compStreamMetric;
}

struct TopologyInfo {
  1: required TopologySummary topology;
  2: required list<ComponentSummary> components;
  3: required list<TaskSummary> tasks;
  4: required TopologyMetric metrics;
}

struct WorkerUploadMetrics {
  1: required string topologyId;
  2: required string supervisorId;
  3: required i32 port;
  4: required MetricInfo allMetrics;
}

struct KillOptions {
  1: optional i32 wait_secs;
}

struct RebalanceOptions {
  1: optional i32 wait_secs;
  2: optional bool reassign;
  3: optional string conf;
}

struct Credentials {
  1: required map<string,string> creds;
}

// topology 初始化状态
enum TopologyInitialStatus {
    ACTIVE = 1,
    INACTIVE = 2
}

// topology 提交选项
struct SubmitOptions {
  1: required TopologyInitialStatus initial_status; // 初始化状态
  2: optional Credentials creds;
}

struct MonitorOptions {
  1: optional bool isEnable;
}



struct ThriftSerializedObject {
  1: required string name;
  2: required binary bits;
}

struct LocalStateData {
   1: required map<string, ThriftSerializedObject> serialized_parts;
}

struct TaskHeartbeat {
    1: required i32 time;
    2: required i32 uptime;
}

struct TopologyTaskHbInfo {
    1: required string topologyId; // topology ID
    2: required i32 topologyMasterId; // topology master ID
    3: optional map<i32, TaskHeartbeat> taskHbs;
}


struct SettableBlobMeta {
  // we remove acl in jstorm
  1: optional i32 replication_factor
}

struct ReadableBlobMeta {
  1: required SettableBlobMeta settable;
  //This is some indication of a version of a BLOB.  The only guarantee is
  // if the data changed in the blob the version will be different.
  2: required i64 version;
}

struct ListBlobsResult {
  1: required list<string> keys;
  2: required string session;
}

struct BeginDownloadResult {
  //Same version as in ReadableBlobMeta
  1: required i64 version;
  2: required string session;
  3: optional i64 data_size;
}

service Nimbus {

  // 提交 topology
  string submitTopology(1: string name, 2: string uploadedJarLocation, 3: string jsonConf, 4: StormTopology topology) throws (1: AlreadyAliveException e, 2: InvalidTopologyException ite, 3: TopologyAssignException tae);
  string submitTopologyWithOpts(1: string name, 2: string uploadedJarLocation, 3: string jsonConf, 4: StormTopology topology, 5: SubmitOptions options) throws (1: AlreadyAliveException e, 2: InvalidTopologyException ite, 3:TopologyAssignException tae);

  // 杀死 topology
  void killTopology(1: string name) throws (1: NotAliveException e);
  void killTopologyWithOpts(1: string name, 2: KillOptions options) throws (1: NotAliveException e);

  // 激活指定 topology
  void activate(1: string name) throws (1: NotAliveException e);

  // 暂停指定 topology
  void deactivate(1: string name) throws (1: NotAliveException e);

  // 重新调度
  void rebalance(1: string name, 2: RebalanceOptions options) throws (1: NotAliveException e, 2: InvalidTopologyException ite);
  void metricMonitor(1: string name, 2: MonitorOptions options) throws (1: NotAliveException e);
  void restart(1: string name, 2: string jsonConf) throws (1: NotAliveException e, 2: InvalidTopologyException ite, 3: TopologyAssignException tae);

  string beginCreateBlob(1: string key, 2: SettableBlobMeta meta) throws (1: KeyAlreadyExistsException kae);
  string beginUpdateBlob(1: string key) throws (1: KeyNotFoundException knf);
  void uploadBlobChunk(1: string session, 2: binary chunk);
  void finishBlobUpload(1: string session);
  void cancelBlobUpload(1: string session);
  ReadableBlobMeta getBlobMeta(1: string key) throws (1: KeyNotFoundException knf);
  void setBlobMeta(1: string key, 2: SettableBlobMeta meta) throws (1: KeyNotFoundException knf);
  BeginDownloadResult beginBlobDownload(1: string key) throws (1: KeyNotFoundException knf);
  binary downloadBlobChunk(1: string session);
  void deleteBlob(1: string key) throws (1: KeyNotFoundException knf);
  ListBlobsResult listBlobs(1: string session); //empty string "" means start at the beginning
  i32 getBlobReplication(1: string key) throws (1: KeyNotFoundException knf);
  i32 updateBlobReplication(1: string key, 2: i32 replication) throws (1: KeyNotFoundException knf);
  void createStateInZookeeper(1: string key); // creates state in zookeeper when blob is uploaded through command line

  // need to add functions for asking about status of storms, what nodes they're running on, looking at task logs

  //@deprecated blobstore does these
  void beginLibUpload(1: string libName);

  // 上传文件
  string beginFileUpload();
  void uploadChunk(1: string location, 2: binary chunk);
  void finishFileUpload(1: string location);

  // 下载文件
  string beginFileDownload(1: string file);
  //can stop downloading chunks when receive 0-length byte array back
  binary downloadChunk(1: string id);
  void finishFileDownload(1: string id);

  // returns json
  string getNimbusConf(); // 获取配置项，json 字符串
  string getStormRawConf();
  string getSupervisorConf(1: string id);

  //returns json
  string getTopologyConf(1: string id) throws (1: NotAliveException e); // 获取指定 topology 配置项，json 字符串
  string getTopologyId(1: string topologyName) throws (1: NotAliveException e);

  // stats functions
  ClusterSummary getClusterInfo();  // 获取当前集群总体统计信息
  SupervisorWorkers getSupervisorWorkers(1: string host) throws (1: NotAliveException e);
  SupervisorWorkers getSupervisorWorkersById(1: string id) throws (1: NotAliveException e);

  TopologyInfo getTopologyInfo(1: string id) throws (1: NotAliveException e);

  // 获取指定名称的 topology 信息
  TopologyInfo getTopologyInfoByName(1: string topologyName) throws (1: NotAliveException e);
  map<i32, string> getTopologyTasksToSupervisorIds(1: string topologyName) throws (1: NotAliveException e);
  map<string, map<string,string>> getTopologyWorkersToSupervisorIds(1: string topologyName) throws (1: NotAliveException e);

  // 获取系统 topology（在用户提交的 topology 基础上添加 acker、metric 等系统定义 bolt 后形成的 topology）
  StormTopology getTopology(1: string id) throws (1: NotAliveException e);
  // 获取用户提交的 topology
  StormTopology getUserTopology(1: string id) throws (1: NotAliveException e);
  void notifyThisTopologyTasksIsDead(1: string topologyId);

  // relate metric
  void uploadTopologyMetrics(1: string topologyId, 2: TopologyMetric topologyMetrics);
  map<string,i64> registerMetrics(1: string topologyId, 2: set<string> metrics);
  TopologyMetric getTopologyMetrics(1: string topologyId);

  // get metrics by type: component/task/stream/worker/topology
  list<MetricInfo> getMetrics(1: string topologyId, 2: i32 type);

  MetricInfo getNettyMetrics(1:string topologyId);
  MetricInfo getNettyMetricsByHost(1: string topologyId, 2: string host);
  MetricInfo getPagingNettyMetrics(1: string topologyId, 2: string host, 3: i32 page);
  i32 getNettyMetricSizeByHost(1: string topologyId, 2: string host);

  MetricInfo getTaskMetrics(1:string topologyId, 2:string component);
  list<MetricInfo> getTaskAndStreamMetrics(1:string topologyId, 2: i32 taskId);

  // get only topology level metrics
  list<MetricInfo> getSummarizedTopologyMetrics(1: string topologyId);

  string getVersion();

  void updateTopology(1: string name, 2: string uploadedLocation, 3: string updateConf) throws (1: NotAliveException e, 2: InvalidTopologyException ite);

  void updateTaskHeartbeat(1: TopologyTaskHbInfo taskHbs);

  void setHostInBlackList(1: string host);
  void removeHostOutBlackList(1: string host);

  void deleteMetricMeta(1: string topologyId, 2: i32 metaType, 3: list<string> idList);

  // gray upgrade and rollback
  void grayUpgrade(1: string topologyName, 2: string component, 3: list<string> workers, 4: i32 workerNum);
  void rollbackTopology(1: string topologyName);
  void completeUpgrade(1: string topologyName);
}

struct DRPCRequest {
  // 函数的参数列表
  1: required string func_args;
  // 请求的 request_id
  2: required string request_id;
}

exception DRPCExecutionException {
  1: required string msg;
}

service DistributedRPC {
  string execute(1: string functionName, 2: string funcArgs) throws (1: DRPCExecutionException e, 2: AuthorizationException aze);
}

service DistributedRPCInvocations {
  void result(1: string id, 2: string result) throws (1: AuthorizationException aze);
  DRPCRequest fetchRequest(1: string functionName) throws (1: AuthorizationException aze);
  void failRequest(1: string id) throws (1: AuthorizationException aze);  
}
