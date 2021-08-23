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

package com.intel.featureStorage;

import com.intel.featureStorage.entities.Feature;
import com.intel.featureStorage.utils.Utils;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.codec.DecoderException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.test.context.junit4.SpringRunner;
import java.io.IOException;

//@RunWith(SpringRunner.class)
//@SpringBootTest
//@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HbaseClientTest {

//    private HbaseClient hBaseClient;
//
//    private final String tableName = "vehicle_feature_test";
//
//    private static List<byte[]> rowKeys = new ArrayList<>();
//
//    private final static long[] ts = new long[]{1605256289271L, 1605256495309L, 1605256673131L};
//
//    private final static int recordLength = ts.length;
//
//    @Autowired
//    public void setHbaseClient(HbaseClient client){
//        this.hBaseClient = client;
//    }
//
//    @PostConstruct
//    private void init() {
//        // prepare data
//        try {
//            for (int i = 0; i < recordLength; i++) {
//                //long ts = System.currentTimeMillis();
//                byte[] b = HbaseClient.genRowKey(ts[i]);
//                rowKeys.add(b);
//            }
//        }catch(Exception e)
//        {
//            e.printStackTrace();
//        }
//    }
//
//    @PreDestroy
//    public void destroy() throws IOException {
//        hBaseClient.close();
//    }
//
//    @Test
//    public void test2CreateTable() throws IOException {
//        //hBaseClient.dropTable(tableName);
//        hBaseClient.createTable(tableName);
//    }
//
//    @Test
//    public void test3PutBatch() throws IOException, NoSuchAlgorithmException {
//        List<Put> puts = new Vector<>();
//        byte[] vector = new byte[512];
//        for(int i =0; i<recordLength; i++) {
//            byte[] b = rowKeys.get(i);
//            System.out.println(DatatypeConverter.printBase64Binary(b));
//            Put put = new Put(b);
//            put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("media_uri"),
//                Bytes.toBytes("uuid" + i));
//            put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("roi_x"),
//                Utils.intToBytes(new Random().nextInt(100)));
//            put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("roi_y"),
//                Utils.intToBytes(new Random().nextInt(100)));
//            put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("roi_w"),
//                Utils.intToBytes(new Random().nextInt(100)));
//            put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("roi_h"),
//                Utils.intToBytes(new Random().nextInt(100)));
//            SecureRandom.getInstanceStrong().nextBytes(vector);
//            put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("feature_vector"),
//                Bytes.toBytes(DatatypeConverter.printBase64Binary(vector)));
//            puts.add(put);
//        }
//        long l = hBaseClient.put(tableName, puts);
//        System.out.format("Completed batch put test with %d records and %d second used %n",
//            puts.size(), l*1000);
//    }
//
//    @Test
//    public void test5Delete() throws IOException {
//        hBaseClient.delete(tableName, DatatypeConverter.printBase64Binary(rowKeys.get(0)));
//    }
//
//    @Test
//    public void test4Get() throws IOException, DecoderException {
//        System.out.println(DatatypeConverter.printBase64Binary(rowKeys.get(0)));
//        Feature e = hBaseClient.get(tableName, rowKeys.get(0), Feature.class);
//        System.out.println(e.getUnixTimestamp());
//    }
//
//    @Test
//    public void test1DropTable() throws IOException {
//        hBaseClient.dropTable(tableName);
//    }

}
