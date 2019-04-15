package cn.rdtimes.disb.test;

import cn.rdtimes.disb.core.BAbstractJob;

import java.util.concurrent.CountDownLatch;

/**
 * Created by BZ.
 */
public class BTestNodeJob extends BAbstractJob {
    private CountDownLatch countDownLatch;

    public BTestNodeJob() {
        countDownLatch = new CountDownLatch(1);
    }

    public void start() throws Exception {
        Thread tt = new Thread(new Runnable() {
            public void run() {
                int min = split.getInt("min");
                int max = split.getInt("max");

                System.out.println("split min=" + min + ",max=" + max);

                try {
                    Thread.sleep(10000);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                countDownLatch.countDown();
            }
        });
        tt.start();
    }

    public void stop() {
        countDownLatch.countDown();
    }

    public void waitCompleted() {
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
