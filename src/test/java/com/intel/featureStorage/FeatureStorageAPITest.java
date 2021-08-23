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

import com.intel.featureStorage.entities.ClusterFeature;
import com.intel.featureStorage.entities.Feature;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.test.context.junit4.SpringRunner;

//@RunWith(SpringRunner.class)
//@SpringBootTest
public class FeatureStorageAPITest {
//  public static FeatureStorageProperties config;
//  public static FeatureStorageAPI api;
//
//  @Autowired
//  public void setFeatureStorageProperties(FeatureStorageProperties config) {
//    this.config = config;
//  }
//
//  @Autowired
//  public void setFeatureStorageAPI(FeatureStorageAPI api){
//    this.api = api;
//  }
//
//  @Test
//  public void test1GetAllDistricts() throws Exception {
//    String[] d = api.getAllDistricts();
//    for(String t: d){
//      System.out.println(t);
//    }
//  }
//
//  @Test
//  public void test2GetMediaUrlByIDs() throws Exception {
//    String[] fvids = new String[2];
//    fvids[0] = "AAAAAAF1+FBDnjliYmYxMzNjN2Y2YTQwZTc=";
//    fvids[1] = "AAAAAAF1+FBDnjliYmYxMzNjN2Y2YTQwZTd=";
//    Map<String,String> ret = api.getMediaUrlByIDs("vehicle", fvids, new String[]{""});
//    for (Map.Entry<String, String> entry : ret.entrySet()) {
//      System.out.println(entry.getKey() + ":" + entry.getValue());
//    }
//  }
//
//  @Test
//  public void test3queryBehavior() throws Exception {
//    String[] fvids = new String[2];
//    fvids[0] = "AAAAAAF1+FBDnjliYmYxMzNjN2Y2YTQwZTc=";
//    fvids[1] = "AAAAAAF1+FBDnjliYmYxMzNjN2Y2YTQwZTd=";
//    String constraints = "media_meta.timestamp <= 1606188811169 "
//        + "AND media_meta.timestamp >= 1606188811169 "
//        + "AND capture_source.district in ('闵行', '浦东新区')";
//
//    String filters = "media_meta.timestamp, occurrence.media_uri, capture_source.latitude, "
//        + "capture_source.longitude";
//    List<Feature> ret = api.queryBehavior("structured_data", fvids,
//        "vehicle", constraints, filters);
//    System.out.println(ret.size());
//  }
//
//  @Test
//  public void test4getArchivedFeatures() throws Exception {
//    String filters = "archive.license_plate, vehicle_registration.color, "
//        + "vehicle_registration.brand, vehicle_registration.model";
//    String[] vids = new String[2];
//    vids[0] = "1234567890123456890123456789012";
//    vids[1] = "1234567890123456890123456789013";
//    String[] datasources = {"structured_data"};
//    List<ClusterFeature> ret = api.getArchivedFeatures("vehicle",
//        datasources, vids, filters);
//    System.out.println(ret.size());
//  }

}
