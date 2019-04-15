package cn.rdtimes.disb.test;

import cn.rdtimes.disb.client.BJob;

/**
 * Created by BZ.
 */
public class BTestMain {

    public static void main(String[] args) throws Exception {
        BJob clientJob = new BJob();
        clientJob.setMasterIp("127.0.0.1").setMasterPort(21998);
        clientJob.setInputSplitClass(BTestInputSplit.class);
        clientJob.setJobClass(BTestMultiThreadJob.class);     //(BTestNodeJob.class);

        clientJob.launchJob();
        clientJob.waitCompleted();

        System.out.println("Job is over===============" + clientJob);
    }

}
