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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple Spark demo
 */
public final class SparkTest {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("SparkTest1").setMaster("local[*]");
//        SparkConf sparkConf = new SparkConf()
//                .setAppName("SparkTest1")
//                .setMaster("spark://127.0.0.1:7077")
//                .set("spark.cores.max", "2")
//                .setJars(new String[]{"map-reduce-exec-spark-1.0.1.jar"});
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        int num = 100000;
        List<Integer> list = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            list.add(i + 1);
        }

        final JavaRDD<Integer> data = ctx.parallelize(list, 8);
        int res = data.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer i1) throws Exception {
                return i1;
            }
        })
                .reduce(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) throws Exception {
                        return i1 + i2;
                    }
                });

        System.out.println(res);
        ctx.stop();
    }

}
