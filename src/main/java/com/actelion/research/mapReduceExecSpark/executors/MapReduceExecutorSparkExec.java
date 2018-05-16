/*
 *     Orbit, a versatile image analysis software for biological image-based quantification.
 *     Copyright (C) 2009 - 2018 Actelion Pharmaceuticals Ltd., Gewerbestrasse 16, CH-4123 Allschwil, Switzerland.
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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.*;


/**
 * Executes a MapReduce via Spark (cluster mode, e.g. Mesos). Will be called by MapReduceExecutorSparkProxy on cluster side.
 *
 * @param <T> Type Input (e.g. Integer for IDs)
 * @param <K> Output Key (measurement identifier)
 * @param <V> Output Value (e.g. Integer for a count)
 */
public final class MapReduceExecutorSparkExec<T, K, V> implements IMapReduceExecutor<T, K, V>, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(MapReduceExecutorSparkExec.class);
    private double progress = 0d;
    private String appName = "MapReduceExecutorSpark";
    private String master = "local[*]";
    private transient String[] jars = new String[]{};
    private String taskUUID ;
    private String jobDescription = "Orbit";
    private String jobGroupId = "Orbit";


    public MapReduceExecutorSparkExec(String input, String resultDir, String smbUsername, String smbPassword, String smbDomain, String smbShare) {
        this.taskUUID = UUID.randomUUID().toString();
        InputStream is = null;
        try  {
            if (input.toLowerCase().startsWith("http")) {
                is = new URL(input).openStream();
            } else {
                // assume samba
                SmbUtils smb = new SmbUtils(smbUsername,smbPassword,smbDomain,smbShare);
                byte[] bytes = smb.readFromRemote(input);
                is = new ByteArrayInputStream(bytes);
            }

            ObjectInputStream ois = new ObjectInputStream(is);
            IMapReduce mapReduce = (IMapReduce) ois.readObject();
            Collection elements = (Collection) ois.readObject();

            StringBuilder sb = new StringBuilder("Orbit:"+taskUUID+":"+mapReduce.getClass().getSimpleName()+" [");
            if (elements!=null) {
                int maxElements = 10;
                Iterator iter = elements.iterator();
                int i=0;
                while (iter.hasNext() && i<maxElements) {
                    i++;
                    sb.append(iter.next());
                    if (iter.hasNext() && i<maxElements) {
                        sb.append(";");
                    }
                }
            }
            sb.append("]");
            jobDescription = sb.toString();

            Map map = execute(elements,mapReduce);
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(os);
            oos.writeObject(map);
            oos.flush();
            byte[] bytes = os.toByteArray();

            String dir = getCurrentDir()+"/orbit/results";
            new File(dir).mkdirs();
            File f = new File(dir+File.separator+taskUUID);
            logger.info("writing result to: "+f.getAbsolutePath());

            try (FileOutputStream fos = new FileOutputStream(f)) {
                fos.write(bytes);
                fos.flush();
            }
            // store on remote context store
            IRemoteContextStore resultSaver = new SmbUtils(smbUsername,smbPassword,smbDomain,smbShare);
            resultSaver.copyToRemote(bytes,resultDir, taskUUID);

        } catch (Exception e) {
            e.printStackTrace();
        }  finally {
            if (is!=null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
                           
    }

    public static String getCurrentDir() {
        String path = "";
        try {
            path = System.getProperty("user.dir");
            String sandbox = System.getenv("MESOS_SANDBOX");
            if (sandbox!=null && sandbox.length()>0) {
                path = sandbox;
            }
        }  catch (Exception e) {}
        return path;
    }


    public Map<K, V> execute(final Collection<T> elements, final IMapReduce<T, K, V> mapReduce) {
        long startTime = System.currentTimeMillis();

        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .getOrCreate();
        JavaSparkContext ctx = new JavaSparkContext(spark.sparkContext());
        ctx.setJobGroup(jobGroupId,jobDescription);

//      for local execution
//      SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(master).setJars(jars);
//      JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        List<KeyValue<K, V>> results = ctx
                .parallelize(new ArrayList<>(elements)/*,elements.size()*/)
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

        setProgress(100d);
        double usedTime = (System.currentTimeMillis() - startTime) / 1000d;
        System.out.println("Used time: " + usedTime + " s");
        return reduced;
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


    public static void main(String[] args) throws Exception {
        if (args==null||args.length<1) {
            throw new Exception("No arguments. Serialization url as argument expected.");
        }
        String resultDir = "sparkresults";
        if (args.length>1)
            resultDir = args[1];
        System.out.println("result dir: "+resultDir);
        new File(resultDir).mkdirs();

        String smbUsername = "";
        if (args.length>2)
            smbUsername = args[2];
        System.out.println("smbUsername: "+smbUsername);

        String smbPassword = "";
        if (args.length>3)
            smbPassword = args[3];
        System.out.println("smbPassword: "+smbPassword);

        String smbDomain = "";
        if (args.length>4)
            smbDomain = args[4];
        System.out.println("smbDomain: "+smbDomain);

        String smbShare = "";
        if (args.length>5)
            smbShare = args[5];
        System.out.println("smbShare: "+smbShare);

        MapReduceExecutorSparkExec mapReduceExecutorSparkExec = new MapReduceExecutorSparkExec(args[0],resultDir,smbUsername,smbPassword,smbDomain,smbShare);
    }
}
