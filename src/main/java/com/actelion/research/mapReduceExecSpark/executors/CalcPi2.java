/*
 *     Orbit, a versatile image analysis software for biological image-based quantification.
 *     Copyright (C) 2009 - 2016 Actelion Pharmaceuticals Ltd., Gewerbestrasse 16, CH-4123 Allschwil, Switzerland.
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
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
public class CalcPi2 implements IMapReduce<Long, String, Double> {
    private Random random = new Random();


    public List<KeyValue<String, Double>> map(Long element) {
        long count = 0;
        long inside = 0;
        for (long l = 0; l < element; l++) {
            count++;
            double x = random.nextDouble();
            double y = random.nextDouble();
            if (Math.hypot(x, y) < 1) inside++;
        }
        double approx = 4d * (inside / (double) count);
        List<KeyValue<String, Double>> result = new ArrayList<KeyValue<String, Double>>();
        result.add(new KeyValue<String, Double>("pi", approx));
        return result;
    }

    public Double reduce(String key, List<Double> valueList) {
        double approx = 0d;
        for (double d : valueList) {
            approx += d;
        }
        approx /= valueList.size();
        System.out.println("pi is approx "+approx+ " list size: "+valueList.size());
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
        for (int i = 0; i < 100; i++) input.add(100000L);
        //IMapReduceExecutor<Long,String,Double> executor = new MapReduceExecutorLocal<Long, String, Double>();
        //IMapReduceExecutor<Long, String, Double> executor = new MapReduceExecutorLocalMultiCore<Long, String, Double>();
        IMapReduceExecutor<Long, String, Double> executor = new MapReduceExecutorSpark<>("test1","local[*]",null);
        Map<String, Double> result = executor.execute(input, new CalcPi2());
        for (String s : result.keySet()) {
            System.out.println(s + ": " + result.get(s));
        }

    }


}
