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
import com.intel.featureStorage.FeatureStorageConfig;
//import com.intel.featureStorage.FeatureStorageProperties;
import com.intel.featureStorage.utils.GeoLocation;
import com.intel.featureStorage.utils.RandomGUID;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.xml.bind.DatatypeConverter;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;
//import org.springframework.beans.factory.annotation.Autowired;

//@HBTable(name="vehicle_feature", families = {@Family(name = "t"),})
@HBTable("vehicle_feature")
public class Feature implements HBRecord {

    public void setUnixTimestamp(long unixTimestamp) {
        this.unixTimestamp = unixTimestamp;
    }

    public void setMediaUri(String mediaUri) {
        this.mediaUri = mediaUri;
    }

    public void setMediaUrl(String mediaUrl) {
        this.mediaUrl = mediaUrl;
    }

    public void setGeolocation(GeoLocation geolocation) {
        this.geolocation = geolocation;
    }

//    @Autowired
//    private FeatureStorageProperties config;

    @HBRowKey
    private long unixTimestamp; // precision to millisecond

    @HBColumn(family = "t", column = "media_uri")
    private String mediaUri;
    @HBColumn(family = "t", column = "roi_x")
    private Integer roiX;
    @HBColumn(family = "t", column = "roi_y")
    private Integer roiY;
    @HBColumn(family = "t", column = "roi_w")
    private Integer roiW;
    @HBColumn(family = "t", column = "roi_h")
    private Integer roiH;
    @HBColumn(family = "t", column = "feature_vector") // in base64 string
    private String featureVector;

    private String objectType;
    private String featureVectorID; // rowKey
    private String mediaUrl;
    private JSONObject attributes; // json object
    private GeoLocation geolocation;

    @Override
    public byte[] composeRowKey() {
        String regionNum = FeatureStorageConfig.getConfigValues("region.number");
        short num;
        try {
            num = Short.parseShort(regionNum);
        }catch(Exception e){
            num = 1; // default region number if it does not exist
        }
        short salt = (short)(unixTimestamp % num);
        String uuid = RandomGUID.generate().substring(0, 16);
        // 2 bytes + 8 bytes + 16 bytes
        byte[] rk = Bytes.add(Bytes.toBytes(salt), Bytes.toBytes(unixTimestamp), Bytes.toBytes(uuid));
        this.featureVectorID = DatatypeConverter.printBase64Binary(rk);
        return rk;
    }

    @Override
    public void parseRowKey(byte[] rowKey) {
        this.featureVectorID = DatatypeConverter.printBase64Binary(rowKey);
        // slice from index 2 to index 9
        byte[] slice = Arrays.copyOfRange(rowKey, 2, 10);
        this.unixTimestamp = ByteBuffer.wrap(slice).getLong();
    }

    public Feature() {}

    public Feature(long timestamp, String mediaUri, int roiX, int roiY, int roiH, int roiW,
        byte[] featureVector) {
        this.unixTimestamp = timestamp;
        this.mediaUri = mediaUri;
        this.roiX = roiX;
        this.roiY = roiY;
        this.roiH = roiH;
        this.roiW = roiW;
        this.featureVector = DatatypeConverter.printBase64Binary(featureVector);
    }

    // getter
    public long getUnixTimestamp() {
        return unixTimestamp;
    }

    public String getMediaUri() {
        return mediaUri;
    }

    public Integer getRoiX() {
        return roiX;
    }

    public Integer getRoiY() {
        return roiY;
    }

    public Integer getRoiW() {
        return roiW;
    }

    public Integer getRoiH() {
        return roiH;
    }

    public byte[] getFeatureVector() {
        return DatatypeConverter.parseBase64Binary(this.featureVector);
    }

    public String getObjectType() {
        return objectType;
    }

    public String getFeatureVectorID() {
        return featureVectorID;
    }

    public String getMediaUrl() {
        return mediaUrl;
    }

    public JSONObject getAttributes() {
        return attributes;
    }

    public GeoLocation getGeolocation() {
        return geolocation;
    }
}

//public class Feature {
//
//  /**
//   * specify the object type of the feature
//   */
//  public String objectType;
//
//  /**
//   * The key or rowKey of the row
//   */
//  public String featureVectorID; //rowKey
//
//  public int[] featureVector;
//  public String mediaUri; // uuid, optional
//  public String mediaUrl; //
//  public JSONObject attributes; //json object
//  public Timestamp timestamp;
//  public GeoLocation geolocation;
//}