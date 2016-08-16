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

package com.actelion.research.mapReduceExecSpark.test;

import com.actelion.research.mapReduceExecSpark.executors.MapReduceExecutorSpark;
import com.actelion.research.mapReduceGeneric.examples.CalcPi;
import com.actelion.research.mapReduceGeneric.examples.ComputeSum;
import com.actelion.research.mapReduceGeneric.executors.IMapReduceExecutor;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

/**
 * Computes the sum and the product of a set of numbers. Demonstrates how to compute several key/value pairs.
 */
public class testComputeSumSpark {

    private ComputeSum computeSum = new ComputeSum();
    private CalcPi calcPi = new CalcPi();
    private String master = "local[*]";
    private String[] jars = new String[]{};   // no jars for local execution

    @Test
    public void testComputesumSpark() throws Exception {
        IMapReduceExecutor<Integer, String, Long> executorSpark = new MapReduceExecutorSpark<>("sumTest", master, jars);
        Map<String, Long> reduced = executorSpark.execute(Arrays.asList(1, 2, 3, 4, 5), computeSum);
        for (String key : reduced.keySet()) {
            System.out.println("res " + key + ": " + reduced.get(key));
        }
        assertEquals(120L, (long) reduced.get("mult"));
        assertEquals(15L, (long) reduced.get("sum"));
    }


    @Test
    public void testComputePiSpark() throws Exception {
        IMapReduceExecutor<Long, String, Double> executorSpark = new MapReduceExecutorSpark<>("piTest", master, jars);
        List<Long> input = new ArrayList<Long>();
        for (int i = 0; i < 100; i++) input.add(1000000L);
        Map<String, Double> reduced = executorSpark.execute(input, calcPi);
        for (String key : reduced.keySet()) {
            System.out.println("res " + key + ": " + reduced.get(key));
        }
        assertEquals(3.14d, reduced.get("pi"), 0.05d);

    }


}
