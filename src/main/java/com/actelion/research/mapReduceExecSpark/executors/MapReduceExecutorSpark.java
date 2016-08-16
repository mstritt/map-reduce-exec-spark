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
import com.actelion.research.mapReduceGeneric.IRemoteContextStore;
import com.actelion.research.mapReduceGeneric.executors.IMapReduceExecutor;
import com.actelion.research.mapReduceGeneric.utils.KeyValue;
import com.actelion.research.mapReduceGeneric.utils.TaskResultGeneric;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPOutputStream;


/**
 * Executes a MapReduce via Spark.
 * By default appName is MapReduceExecutorSpark and master is local[*]. To change it set appName and master variables.
 *
 * @param <T> Type Input (e.g. Integer for IDs)
 * @param <K> Output Key (measurement identifier)
 * @param <V> Output Value (e.g. Integer for a count)
 */
public final class MapReduceExecutorSpark<T, K, V> implements IMapReduceExecutor<T, K, V>, Serializable {

    private double progress = 0d;
    private String appName = "MapReduceExecutorSpark";
    private String master = "local[*]";
    private transient String[] jars = new String[]{};
    private transient SparkConf sparkConf = null;
    private transient IRemoteContextStore resultSaver = null; // optional
    private transient String remoteFolder;

    public MapReduceExecutorSpark() {
    }

    public MapReduceExecutorSpark(String appName, String master, String[] jars) {
        this.appName = appName;
        this.master = master;
        if (jars != null)
            this.jars = jars;
    }

    public MapReduceExecutorSpark(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
    }

    public Map<K, V> execute(final Collection<T> elements, final IMapReduce<T, K, V> mapReduce) {
        long startTime = System.currentTimeMillis();

        if (sparkConf == null) {
            sparkConf = new SparkConf().setAppName(appName).setMaster(master).setJars(jars);
        } else {
            // sparkConf already set, however, set additional jars
            if (jars != null && jars.length > 0) {
                sparkConf = sparkConf.setJars(jars);
            }
        }
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        List<KeyValue<K, V>> results = ctx.parallelize(new ArrayList<>(elements)/*,elements.size()*/)
                .map(new Function<T, List<KeyValue<K, V>>>() {
                    public List<KeyValue<K, V>> call(T t) throws Exception {
                        return mapReduce.map(t);
                    }
                })
                .reduce(new Function2<List<KeyValue<K, V>>, List<KeyValue<K, V>>, List<KeyValue<K, V>>>() {
                    public List<KeyValue<K, V>> call(List<KeyValue<K, V>> keyValues, List<KeyValue<K, V>> keyValues2) throws Exception {
                        List<KeyValue<K, V>> kvList = new ArrayList<KeyValue<K, V>>();
                        if (keyValues != null) kvList.addAll(keyValues);
                        if (keyValues2 != null) kvList.addAll(keyValues2);
                        if (kvList.size() == 0) {
                            System.out.println("Warning: Map step returned no results");
                        }

                        // merge keys
                        final Map<K, List<V>> kvResults = new HashMap<K, List<V>>();
                        for (KeyValue<K, V> keyValue : kvList) {
                            if (!kvResults.containsKey(keyValue.getKey())) {
                                kvResults.put(keyValue.getKey(), new ArrayList<V>());
                            }
                            kvResults.get(keyValue.getKey()).add(keyValue.getValue());
                        }

                        // reduce
                        List<KeyValue<K, V>> kvMap = new ArrayList<KeyValue<K, V>>();
                        for (K key : kvResults.keySet()) {
                            kvMap.add(new KeyValue<K, V>(key, mapReduce.reduce(key, kvResults.get(key))));
                        }
                        return kvMap;
                    }
                });


        final Map<K, V> reduced = new HashMap<K, V>();

        for (KeyValue<K, V> kv : results) {
            if (reduced.containsKey(kv.getKey())) {
                System.out.println("Error: duplicate key after reduce!!!");
            }
            reduced.put(kv.getKey(), kv.getValue());
        }

        ctx.close();

        // write to remote share (e.g. samba share)
        setProgress(98d);
        if (resultSaver != null) {
            String filename = "Orbit-MapReduce-" + new SimpleDateFormat("dd.MM.yyyy_HH.mm.ss").format(new Date()) + "-" + UUID.randomUUID().toString().substring(0, 6) + ".gz";
            TaskResultGeneric taskResultGeneric = new TaskResultGeneric(mapReduce.getClass().getName(), reduced, new Date().getTime(), null, 0);
            byte[] bytes = getAsBytes(taskResultGeneric);
            try {
                resultSaver.copyToRemote(bytes, remoteFolder, filename);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        setProgress(100d);
        double usedTime = (System.currentTimeMillis() - startTime) / 1000d;
        System.out.println("Used time: " + usedTime + " s");
        return reduced;
    }


    private byte[] getAsBytes(Object map) {
        ByteArrayOutputStream outputStream = null;
        GZIPOutputStream zip = null;
        ObjectOutputStream oos = null;
        try {
            outputStream = new ByteArrayOutputStream();
            zip = new GZIPOutputStream(outputStream);
            oos = new ObjectOutputStream(zip);
            oos.writeObject(map);
            zip.finish();
            oos.flush();
            zip.flush();
            outputStream.flush();
            return outputStream.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();  // bad luck
            return null;
        } finally {
            try {
                if (oos != null) oos.close();
                if (zip != null) zip.close();
                if (outputStream != null) outputStream.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
    }


    public double getProgress() {
        return progress;
    }

    protected void setProgress(double progress) {
        this.progress = progress;
    }

    public void cancel() {
        // not implemented
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String[] getJars() {
        return jars;
    }

    public void setJars(String[] jars) {
        this.jars = jars;
    }

    public SparkConf getSparkConf() {
        return sparkConf;
    }

    public void setSparkConf(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
    }


    /**
     * Optional: set to store the results (gzipped binary serialized key-value map) to a share.
     */
    public void setResultSaver(IRemoteContextStore resultSaver, String remoteFolder) {
        this.resultSaver = resultSaver;
        this.remoteFolder = remoteFolder;
    }
}
