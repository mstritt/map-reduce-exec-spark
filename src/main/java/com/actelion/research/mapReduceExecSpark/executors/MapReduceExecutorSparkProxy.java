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
 * Date: 4/17/18 1:55 PM
 */

package com.actelion.research.mapReduceExecSpark.executors;

import com.actelion.research.mapReduceGeneric.IMapReduce;
import com.actelion.research.mapReduceGeneric.executors.IMapReduceExecutor;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.*;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;


/**
 * Executes a MapReduce implementation via Spark on a remote cluster.
 * The MapReduce class will be serialized and transferred to the cluster via a samba share.
 * On cluster side an executor will deserialize it and execute the Spark task in cluster-mode.
 * No results will be returned. This has to be done manually by reading the serialized result (Map<K, V>) from the samba share.
 *
 * Your fatJar must contain the map-reduce-exec-spark dependency!
 *
 * @param <T> Type Input (e.g. Integer for IDs)
 * @param <K> Output Key (measurement identifier)
 * @param <V> Output Value (e.g. Integer for a count)
 */
public final class MapReduceExecutorSparkProxy<T, K, V> implements IMapReduceExecutor<T, K, V>, Serializable {

    private double progress = 0d;
    private String appName = "MapReduceExecutorSpark";
    private transient String[] jars = new String[]{};
    private int totalClusterCores = 330;
    private double clusterUsage = 0.75;
    private int coresPerJob = 10;
    private int memPerJobGB = 62;
    private String fatJarName;
    private String resultDir;
    private String smbUsername;
    private String smbPassword;
    private String smbDomain;
    private String smbShare;
    private String host_port;
    private final SmbUtils smbUtils;


    public MapReduceExecutorSparkProxy(String host_port, String appName, int coresPerJob, int memPerJobGB, double clusterUsage, String fatJarName, String resultDir, String smbUsername, String smbPassword, String smbDomain, String smbShare) {
        this.appName = appName;
        //if (jars != null) this.jars = jars;
        this.coresPerJob = coresPerJob;
        this.memPerJobGB = memPerJobGB;
        this.clusterUsage = clusterUsage;
        this.fatJarName = fatJarName;
        this.resultDir = resultDir;
        this.smbUsername = smbUsername;
        this.smbPassword = smbPassword;
        this.smbDomain = smbDomain;
        this.smbShare = smbShare;
        this.host_port = host_port;
        this.smbUtils = new SmbUtils(smbUsername,smbPassword,smbDomain,smbShare);
    }

    public Map<K, V> execute(final Collection<T> elements, final IMapReduce<T, K, V> mapReduce) {
        String serUUID = UUID.randomUUID().toString();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] bytes = null;
        try {
            ObjectOutputStream oos = new ObjectOutputStream(outputStream);
            oos.writeObject(mapReduce);
            oos.writeObject(elements);
            outputStream.flush();
            bytes = outputStream.toByteArray();
            System.out.println("serialization done");
        } catch (IOException e) {
            e.printStackTrace();
        }

        writeToSMB(serUUID, bytes);

        System.out.println("executing task on cluster");
        try {
            deployTask(serUUID, elements.size(),resultDir);
            System.out.println("task deployment successfull");
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public void writeToSMB(String serUUID, byte[] bytes) {
        String path = "spark/MR-"+serUUID+".ser";
        try  {
            smbUtils.copyToRemote(bytes,"spark","MR-"+serUUID+".ser");
            System.out.println("serialized map-reduce task to samba share: "+path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeToFile(String path, String serUUID, byte[] bytes) {
        //String path = httproot + File.separator + serUUID;
        System.out.println("serializing map-reduce task to "+path);
        try  {
            FileOutputStream fileOutputStream = new FileOutputStream(path);
            fileOutputStream.write(bytes);
            fileOutputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void deployTask(String serUUID, int parallelism, String resultDir) throws IOException {
        int numCPUs = Math.min((int)(totalClusterCores*clusterUsage), coresPerJob*parallelism);

        String payload = "{" +
                "  \"action\": \"CreateSubmissionRequest\"," +

                "  \"mainClass\": \"com.actelion.research.mapReduceExecSpark.executors.MapReduceExecutorSparkExec\"," +
                "  \"appArgs\": [ \"spark/MR-"+serUUID+".ser\",\""+resultDir+"\", \""+this.smbUsername+"\",\""+this.smbPassword+"\",\""+this.smbDomain+"\",\""+this.smbShare+"\" ]," +
                "  \"appResource\": \""+fatJarName+"\"," +

                "  \"clientSparkVersion\": \"2.1.1\"," +
                "  \"environmentVariables\" : {" +
                "    \"SPARK_ENV_LOADED\" : \"1\"" +
                "    ,\"MESOS_SANDBOX\" : \"/mnt/mesos/sandbox\"" +
                "  }," +
                "  \"sparkProperties\": {" +
              //  "    \"spark.jars\": \"deps.jar\"," +    // does not work
                "    \"spark.app.name\": \""+appName+"\"," +
                "    \"spark.driver.supervise\":\"false\"," +
                "    \"spark.executor.memory\": \""+memPerJobGB+"G\"," +
              //  "    \"spark.executor.cores\": \"10\"," +     //
              //  "    \"spark.executor.instances\": \"3\"," +   // not taken into account
                "    \"spark.cores.max\": \""+numCPUs+"\"," +  
                "    \"spark.task.cpus\": \""+coresPerJob+"\"," +

                "    \"spark.driver.memory\": \"1G\"," +
                "    \"spark.driver.cores\": \"2\"," +
                "    \"spark.default.parallelism\": \""+parallelism+"\"," +        
                "    \"spark.submit.deployMode\":\"cluster\"," +
                "    \"spark.mesos.executor.docker.image\": \"mesosphere/spark:2.1.0-2.2.1-1-hadoop-2.6\"," +
                "    \"spark.mesos.executor.docker.volumes\": \"/arcite/orbit/:orbit:rw\"" +
                "  }" +
                "}";
        StringEntity entity = new StringEntity(payload,
                ContentType.APPLICATION_JSON);

        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost request = new HttpPost("http://"+host_port+"/v1/submissions/create");
        request.setEntity(entity);

        HttpResponse response = httpClient.execute(request);
        System.out.println("Status Code: "+response.getStatusLine().getStatusCode());
        String responseString = IOUtils.toString(response.getEntity().getContent(), "UTF-8");
        System.out.println(responseString);

    }


//    public Map<K,V> readResult() {
//
//    }


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

    public String[] getJars() {
        return jars;
    }

    public void setJars(String[] jars) {
        this.jars = jars;
    }

}
