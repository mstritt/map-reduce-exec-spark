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
 * Date: 3/19/18 5:17 PM
 */

package com.actelion.research.mapReduceExecSpark.executors;

import com.actelion.research.mapReduceGeneric.IMapReduce;
import com.actelion.research.mapReduceGeneric.executors.IMapReduceExecutor;
import com.actelion.research.mapReduceGeneric.utils.Helpers;
import com.actelion.research.mapReduceGeneric.utils.KeyValue;

import java.util.*;

/**
 * Calculates Pi using a IMapReduceExecutor. Try to use either the Local or LocalMultiCore executor to see the difference.
 */
public class CalcTest implements IMapReduce<Long, String, Double> {
    private Random random = new Random();


    public List<KeyValue<String, Double>> map(Long element) {
        try {
            Thread.sleep(60*1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        List<KeyValue<String, Double>> result = new ArrayList<KeyValue<String, Double>>();
        result.add(new KeyValue<String, Double>("pi", 1d));
        System.out.println("mapper: "+Thread.currentThread());
        return result;
    }

    public Double reduce(String key, List<Double> valueList) {
        double approx = 0d;
        for (double d : valueList) {
            approx += d;
        }
        System.out.println("reducer: test is approx "+approx+ " list size: "+valueList.size()+": "+ Thread.currentThread());
        return approx;
    }

    public Collection<Long> parseParams(String s) {
        return Helpers.parseParamsLong(s);
    }

    public String serializeParam(Long element) {
        return Helpers.serializeParamLong(element);
    }


    public static void main(String[] args) throws Exception {
        List<Long> input = new ArrayList<Long>();
        for (int i = 0; i < 1000; i++) input.add(6L);
        //IMapReduceExecutor<Long,String,Double> executor = new MapReduceExecutorLocal<Long, String, Double>();
        //IMapReduceExecutor<Long, String, Double> executor = new MapReduceExecutorLocalMultiCore<Long, String, Double>();
        IMapReduceExecutor<Long, String, Double> executor = new MapReduceExecutorSpark<>("test1","local[*]",null);
        Map<String, Double> result = executor.execute(input, new CalcTest());
        for (String s : result.keySet()) {
            System.out.println(s + ": " + result.get(s));
        }

    }


}
