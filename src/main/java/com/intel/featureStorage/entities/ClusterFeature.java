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

package com.intel.featureStorage.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;
import com.flipkart.hbaseobjectmapper.HBTable;
import java.util.List;
import javax.xml.bind.DatatypeConverter;
import org.apache.hadoop.hbase.util.Bytes;

@HBTable("archived_vehicle_feature")
public class ClusterFeature implements HBRecord {

  @HBRowKey
  private String uuid;

  @HBColumn(family = "t", column = "feature_rowkeys")
  private List<String> featureRK;

  @HBColumn(family = "t", column = "cluster_center") // in base64 string
  private String clusterCenter;

  @HBColumn(family = "t", column = "media_uri")
  private String mediaUri;

  public void setLicensePlate(String licensePlate) {
    this.licensePlate = licensePlate;
  }

  @HBColumn(family = "t", column = "license_plate")
  private String licensePlate;

  private String attributes;

  public void setAttributes(String attributes) {
    this.attributes = attributes;
  }

  public void setFeatureRK(List<String> featureRK) {
    this.featureRK = featureRK;
  }

  public ClusterFeature() {}

  public ClusterFeature(String uuid, List<String> rowKeys, byte[] clusterCenter, String mediaUri,
      String licensePlate) {
    this.uuid = uuid;
    this.mediaUri = mediaUri;
    this.featureRK = rowKeys;
    this.clusterCenter = DatatypeConverter.printBase64Binary(clusterCenter);
    this.licensePlate = licensePlate;
  }

  @Override
  public byte[] composeRowKey() {
    return Bytes.toBytes(this.uuid);
  }

  @Override
  public void parseRowKey(byte[] rowKey) {
    this.uuid = Bytes.toString(rowKey);
  }

  // getter
  public String getUuid() {
    return uuid;
  }

  public List<String> getFeatureRK() {
    return featureRK;
  }

  public byte[] getClusterCenter() {
    return DatatypeConverter.parseBase64Binary(clusterCenter);
  }

  public String getMediaUri() {
    return mediaUri;
  }

  public String getLicensePlate() {
    return licensePlate;
  }

  public String getAttributes() { return attributes; }
}