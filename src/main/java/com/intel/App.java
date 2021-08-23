/*
 * INTEL CONFIDENTIAL
 * Copyright 2021 Intel Corporation
 *
 * The source code contained or described herein and all documents related to
 * the source code ("Material") are owned by Intel Corporation or its suppliers
 * or licensors. Title to the Material remains with Intel Corporation or
 * its suppliers and licensors. The Material contains trade secrets and
 * proprietary and confidential information of Intel or its suppliers and
 * licensors. The Material is protected by worldwide copyright and trade secret
 * laws and treaty provisions. No part of the Material may be used, copied,
 * reproduced, modified, published, uploaded, posted, transmitted, distributed,
 * or disclosed in any way without Intel's prior express written permission.
 *
 * No license under any patent, copyright, trade secret or other intellectual
 * property right is granted to or conferred upon you by disclosure or delivery
 * of the Materials, either expressly, by implication, inducement, estoppel or
 * otherwise. Any license under such intellectual property rights must be express
 * and approved by Intel in writing.
*/

package com.intel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.intel.featureStorage.FeatureStorageAPI;
import com.intel.featureStorage.entities.ClusterFeature;
import com.intel.featureStorage.FeatureStorageConfig;
import com.intel.featureStorage.HbaseClient;
import com.intel.featureStorage.entities.Feature;
import com.intel.featureStorage.utils.RandomGUID;
import com.intel.featureStorage.utils.Utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

import jakarta.xml.bind.DatatypeConverter;
import org.apache.commons.codec.DecoderException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.hsr.geohash.GeoHash;


public class App implements Runnable{

    private final Logger LOG = LoggerFactory.getLogger(App.class);
    public HbaseClient hbaseClient;
    private final String applicationPath = System.getProperty("user.dir") + "/";
    public FeatureStorageAPI api;
    private final String tableName = "geohash2";
    private final long msPerDay = 24 * 3600 * 1000;
    private final int num = 5000;
    private final int precision = 10;
    private final String filename = "randLocation.txt";

    public App(){
        this.hbaseClient = new HbaseClient();
        this.api = new FeatureStorageAPI();
    }

    String genUUID() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    long[] genTimestamps(long base){
        long[] arr = new long[num];
        for(int i=0; i<num; i++) {
            int offset = i  % num;
            long ts = base + msPerDay * offset / num;
            arr[i] = ts;
        }
        return arr;
    }

    List<byte[]> composeRowkeys() throws IOException {
        List<byte[]> rks = new Vector<>();
        // Saturday, July 10, 2021 12:00:00 AM GMT+08:00 1625846400000
        long base = ZonedDateTime.of(2021,7,10,0,0,0,0,
                ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli();
        long[] ts = genTimestamps(base);
        Path hp = Paths.get(applicationPath, "geohash.txt");
        FileWriter myWriter = new FileWriter(String.valueOf(hp));
        try {
            Path filePath = Paths.get(applicationPath, filename);
            List<String> lines = Files.readAllLines(filePath);
            for(String line: lines){
                String[] geo = line.split(",");
                GeoHash hash = GeoHash.withCharacterPrecision(Double.parseDouble(geo[0]),
                        Double.parseDouble(geo[1]),
                        precision);
                String h = new StringBuilder(hash.toBase32()).reverse().toString();
                LOG.info("Geohash {} for {}, geolocation: {}, {}. ", h, hash.toBase32(), Double.parseDouble(geo[0]),
                        Double.parseDouble(geo[1]));
//                myWriter.write(hash.toBase32() + ',' + Double.parseDouble(geo[0]) + ',' + Double.parseDouble(geo[1]) +
//                        System.getProperty( "line.separator" ));
                for(long t: ts) {
                    String uuid = genUUID().substring(0, 16);
                    rks.add(Bytes.add(Bytes.toBytes(h),
                            Bytes.toBytes(Long.toString(t)),
                            Bytes.toBytes(uuid)));
                    myWriter.write(h + ',' + t + ',' + uuid + System.getProperty( "line.separator" ));
                }

            }
            myWriter.close();
        }
        catch(IOException e){
            LOG.error("Failed to open file {}, {}", applicationPath, e);
            throw new RuntimeException(e);
        }
        return rks;
    }

    public void testWritePerf(){
        try {
//            hbaseClient.dropTable(tableName);
//            hbaseClient.createTable(tableName);

            List<byte[]> rks;
            try {
                rks = composeRowkeys();
            }
            catch(IOException e)
            {
                LOG.error("Failed to generate row keys.");
                throw new RuntimeException(e);
            }

            int recordPerTransaction = 500000;

            if(rks.size() <= recordPerTransaction)
                recordPerTransaction = rks.size();
            LOG.info("recordPerTransaction: {}, {}, ",
                    recordPerTransaction,
                    rks.size()/recordPerTransaction
            );

            long total = 0;
//            List<Thread> list = new ArrayList<>();
//            List<WriteWorker> r = new ArrayList<>();
// Use thread worker
//            for(int j = 0; j < 2; j++) {
//                WriteWorker runnable = new WriteWorker(tableName, recordPerTransaction, rks, j);
//                Thread thread = new Thread(runnable, String.valueOf(j));
//                thread.start();
//                list.add(thread);
//                r.add(runnable);
//            }
//
//            for(int i =0; i < list.size(); i++) {
//                list.get(i).join();
//                LOG.info("Thread {} used {} seconds.", i, r.get(i).getReturnValue());
//                total += r.get(i).getReturnValue();
//            }
            byte[] vector = new byte[512];
            try {
                SecureRandom.getInstance("SHA1PRNG").nextBytes(vector);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            String b64 = DatatypeConverter.printBase64Binary(vector);

//            for(int j = 0; j < rks.size()/recordPerTransaction; j++) {
        for(int j = 0; j < 1; j++) {
            List<Put> puts = new Vector<>();
                for (int i = 0; i < recordPerTransaction; i++) {
                    Put put = new Put(rks.get(recordPerTransaction * j + i));
                    put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("media_uri"),
                            Bytes.toBytes("uuiduuiduuiduuiduuiduuiduuiduuiduuiduuid"));
                    put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("roi_x"),
                            Utils.intToBytes(new Random().nextInt(100)));
                    put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("roi_y"),
                            Utils.intToBytes(new Random().nextInt(100)));
                    put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("roi_w"),
                            Utils.intToBytes(new Random().nextInt(100)));
                    put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("roi_h"),
                            Utils.intToBytes(new Random().nextInt(100)));
                    put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("feature_vector"),
                            Bytes.toBytes(b64));
                    puts.add(put);
                }
                total = hbaseClient.put(tableName, puts);
                LOG.info("Inserted 500000 by using {} time used: {} s", tableName, total * 1.0 /1000);
            }
        }
        catch (Exception e){
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
    }

    public void testReadPerf() throws DecoderException, IOException {
        Path filePath = Paths.get(applicationPath, "random.txt");
        List<byte[]> rks = new Vector<>();
        try (BufferedReader br = new BufferedReader(new FileReader(String.valueOf(filePath.getFileName())))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if(parts.length != 3) {
                    LOG.error("Unexpected input");
                    break;
                }
                byte[] rk = Bytes.add(Bytes.toBytes(parts[0]),
                        Bytes.toBytes(parts[1]),
                        Bytes.toBytes(parts[2]));
                rks.add(rk);
            }
//            LOG.info(String.valueOf(rks.size()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            long currentTime = System.currentTimeMillis();
            List<Feature> rets = hbaseClient.get(tableName, rks, Feature.class);
            long latency = System.currentTimeMillis() - currentTime;
            LOG.info("It took {} s to fetch {} records from 5000000 records.", latency * 1.0/1000, rets.size());
        }
        catch (IOException | DecoderException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {

        try {
            testWritePerf();
//            testReadPerf();
//        } catch (DecoderException e) {
//            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new App().run();
    }
}
