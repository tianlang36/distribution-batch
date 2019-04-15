# distribution-batch

#### 介绍
轻量型分布式批处理框架，用于对大数据集分块处理场景。分为客户端(client)、主服务器(master)和节点服务器(slave)。客户端提交任务到主服务器，主服务器将任务分发给各个节点服务器进行计算。
主服务器主要监控节点服务器在线和计算状态，节点服务器定期发送心跳或任务状态汇报等消息，节点服务器需要独立
部署。本版本未对大量任务并行执行进行严格测试。


#### 软件架构
一个中心（主服务器）和多个节点（节点服务器）结构。
主要原理：将大数据集数据按节点数量进行分片，让节点服务器计算一个分片数据，并将结果存储到指定位置。所有节点
        计算完成表示此任务完成。主服务器与节点服务器通讯使用nio模式。

#### 安装教程
1. JDK1.8及以上，依赖jar包都在lib目录里
2. 打包成jar包即可使用

#### 使用说明

1. 启动主服务器 java cn.rdtimes.disb.master.BMasterMain [confFileName]。要把实现IInputSplit接口相关的类和依赖包放到服务器classpath路径下。
2. 启动各个节点服务器 java cn.rdtimes.disb.slave.BNodeMain [confFileName]。要把实现IJob接口相关的类和依赖包放到各个节点服务器classpath路径下。
3. 客户端再提交任务。如下：

    public static void main(String[] args) throws Exception {

        BJob clientJob = new BJob();
        clientJob.setInputSplit(BTestInputSplit.class);
        clientJob.setJobClass(BTestMultiThreadJob.class);     //(BTestNodeJob.class);
        clientJob.setOutput(true);
        clientJob.launchJob();
        clientJob.waitCompleted();
        System.out.println("Job is over===============" + clientJob);
    }

#### 参与贡献
1. 天狼-BZ

#### 联系方式
1. 邮箱：biz0petter@126.com

