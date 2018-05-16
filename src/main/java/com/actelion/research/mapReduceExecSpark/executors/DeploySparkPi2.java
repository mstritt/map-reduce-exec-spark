/*
 * Copyright 1997-2018 Idorsia Ltd.
 * Hegenheimermattweg 89
 * CH-4123 Allschwil, Switzerland
 *
 * All Rights Reserved.
 * This software is the proprietary information of Idorsia Pharmaceuticals, Ltd.
 * Use is subject to license terms.
 *
 * Author: Manuel Stritt
 * Date: 4/17/18 2:08 PM
 */

package com.actelion.research.mapReduceExecSpark.executors;

import com.actelion.research.mapReduceGeneric.IMapReduce;
import com.actelion.research.mapReduceGeneric.executors.IMapReduceExecutor;

import java.util.ArrayList;
import java.util.List;

public class DeploySparkPi2 {
    public static void main(String[] args) throws Exception {
        IMapReduce<Long, String, Double> mr = new CalcPi2();
        IMapReduceExecutor<Long, String, Double> executor = new MapReduceExecutorSparkProxy<>("myHost:80","SparkPi",1,1,0.75d,"map-reduce-exec-spark-all-1.1.2.jar","res2","smbUser","smbPasswd","smbDomain","smbShare");

        List<Long> input = new ArrayList<Long>();
        for (int i = 0; i < 100; i++) input.add(100000L);
        executor.execute(input,mr);

        // after execution read back the result

    }
}
