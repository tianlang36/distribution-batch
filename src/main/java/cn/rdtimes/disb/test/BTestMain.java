package cn.rdtimes.disb.test;

import cn.rdtimes.disb.client.BJob;
import cn.rdtimes.disb.master.BMasterService;

/**
 * Created by BZ on 2019/2/27.
 */
public class BTestMain {

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

//        BJob clientJob1 = new BJob();
//        clientJob1.setInputSplit(BTestInputSplit.class);
//        clientJob1.setJobClass(BTestMultiThreadJob.class);     //(BTestNodeJob.class);
//        clientJob1.setOutput(true);
//        clientJob1.launchJob();

        clientJob.waitCompleted();
        System.out.println("Job is over===============" + clientJob);

//        clientJob1.waitCompleted();
//        System.out.println("Job is over===============" + clientJob1);

    }

}
