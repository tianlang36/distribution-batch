# distribution-batch

#### 介绍
分布式简单批处理
分为客户端、主服务器和节点服务器。客户端提交任务到主服务器，主服务器将任务分发给各个节点服务器进行计算。
主服务器主要监控节点服务器在线和计算状态，节点服务器定期发送心跳或任务状态汇报等消息。节点服务器需要独立
部署，主服务器这个版本还不支持独立部署，只能和客户端一起使用，只能发布一个任务到节点服务器，完成后可再发布。

下阶段版本将实现主服务器独立部署，可以同时发布多个任务到节点服务器。

#### 软件架构
一个中心（主服务器）和多个节点（节点服务器）结构。
主要原理：将大数据集数据按节点数量进行分片，让节点服务器计算一个分片数据，并将结果存储到指定位置。所有节点
        计算完成表示此任务完成。

#### 安装教程
1. 打包成jar包即可使用

#### 使用说明

1. 先启动各个节点服务器。要把实现的业务处理类或包放到类路径下面
2. 启动主服务器，等待节点服务器连接，客户端再提交任务。如下：

    public static void main(String[] args) throws Exception {

        final BMasterService  masterMain = new BMasterService(args[0]);
        masterMain.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                masterMain.shutdown();
            }
        }));

        try {
            Thread.sleep(10000);
        }catch (Exception e) {
            e.printStackTrace();
        }

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

####联系方式
1. 邮箱：biz0petter@126.com

