package cn.rdtimes.disb.test;

import cn.rdtimes.disb.core.BAbstractJob;
import cn.rdtimes.partition.parallel.BBaseTask;
import cn.rdtimes.partition.parallel.BExecuteContext;
import cn.rdtimes.partition.parallel.BMultiThreadPartition;
import cn.rdtimes.partition.parallel.IPartitionHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by BZ on 2019/3/1.
 */
public class BTestMultiThreadJob extends BAbstractJob {
    private BMultiThreadPartition multiThreadPartition;

    public BTestMultiThreadJob() {
        multiThreadPartition = new BMultiThreadPartition(3);
    }

    public void start() throws Exception {
        multiThreadPartition.run(new PartitionHandlerImp());
    }

    public void waitCompleted() {
        multiThreadPartition.shutdownAwait();
    }

    private class PartitionHandlerImp implements IPartitionHandler {

        public List<BExecuteContext> getPartitions() {
            List<BExecuteContext> ls = new ArrayList<BExecuteContext>(20);
            for (int i = 1; i < 21; i++) {
                BExecuteContext executeContext = new BExecuteContext();
                executeContext.setMin(i);
                executeContext.setMax(i * 1000);

                ls.add(executeContext);
            }
            return ls;
        }

        public Runnable createPartitionTask(BExecuteContext bExecuteContext) {
            return new BTask(bExecuteContext);
        }

    }

    private class BTask extends BBaseTask {
        public BTask(BExecuteContext bExecuteContext) {
            super(bExecuteContext);
        }

        public void process() {
            System.out.println(Thread.currentThread().getName() + " min:" + executeContext.getMin() +
                    "; max:" + executeContext.getMax());
            try {
                Thread.sleep(6000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}
