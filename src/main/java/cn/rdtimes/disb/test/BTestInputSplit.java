package cn.rdtimes.disb.test;

import cn.rdtimes.disb.core.BSplit;
import cn.rdtimes.disb.core.IInputSplit;

/**
 * Created by BZ.
 */
public class BTestInputSplit implements IInputSplit {

    public BSplit[] getSplit(int nodes) throws Exception {
        if (nodes <= 0) return null;

        BSplit[] splits = new BSplit[nodes];
        for(int i = 0; i < nodes; i++) {
            splits[i] = new BSplit();
            splits[i].putInt("min", i);
            splits[i].putInt("max", (i + 1) * 100);
        }

        return splits;
    }

}
