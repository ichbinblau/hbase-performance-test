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

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonObject;
import com.intel.featureStorage.entities.ClusterFeature;
import com.intel.featureStorage.entities.Feature;
import com.intel.featureStorage.utils.GeoLocation;
import com.intel.featureStorage.utils.HttpClientResult;
import com.intel.featureStorage.utils.HttpClientUtils;
import com.intel.featureStorage.utils.MinioSDK;
import io.minio.MinioClient;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Component;

//@Component
public class FeatureStorageAPI {

  private Logger logger = LoggerFactory.getLogger(FeatureStorageAPI.class);

  private FeatureStorageProperties config;

  // feature storage client by reflection
  private FeatureStorageClient client = null;

  private MinioClient minioClient = null;

  private String packageName = null;

  private final String restProto = "http";

  private String restApiBaseUrl = null;

  private final ObjectMapper jsonObjectMapper = new ObjectMapper();

  private final Map<String, String> restUri = new HashMap() {{
    put("getBulkyOccurrenceUri", "/occurrence/%s/bulk_get");
    put("getBulkyArchiveUri", "/archive/%s/bulk_get");
    put("getAllDistricts", "/capture_source/districts");
    put("query", "/query");
  }};

  public void FeatureStorageAPI(){
    // TODO: use factory to produce client
    this.client = new HbaseClient();
  }

  @PostConstruct
  public void init() {
    this.restApiBaseUrl = composeRestUrl();
    this.packageName = config.getProperty("feature.storage.backend");
    this.minioClient = composeMinioclient();

    // try {
    //   this.client = FeatureStorageClientFactory.produce(this.packageName);
    // } catch(Exception e) {
    //   e.printStackTrace();
    // }
  }

  private MinioClient composeMinioclient(){
    String endPoint = config.getProperty("minio.endpoint");
    String accessKey = config.getProperty("minio.access.key");
    String secretKey = config.getProperty("minio.secret.key");
    return MinioSDK.minioClient(endPoint, accessKey, secretKey);
  }

  private String composeRestUrl(){
    String host = config.getProperty("rest.api.host");
    String port = config.getProperty("rest.api.port");
    String version = config.getProperty("rest.api.version");
    if(host.isEmpty()){
      host = "localhost";
    }
    if(port.isEmpty()){
      port = "5000";
    }
    if(version.isEmpty()){
      version = "v1";
    }
    return String.format("%s://%s:%s/%s", restProto, host, port, version);
  }

  /**
   * a generic put feature function
   * @param puts
   * @param objectType
   * @param dataSources
   * @param <T>
   * @return
   */
  public <T> int putFeatures(List<T> puts, String objectType, String[] dataSources) {
    return 0;
  }

  /**
   * for notification service, return attributes by filters or thru sql select
   * @param constraints, including page number and page rows
   * @param filters, the return fields
   * @param dataSources
   * @return feature array
   */
  public List<Feature> selectFeatures(String objectType, String[] dataSources, String[] constraints,
      String[] filters)
      throws Exception {
    if(objectType == null || objectType.isEmpty())
      throw new IllegalArgumentException("Object type cannot be null or empty.");
    int i = 0;
    List<Feature> ret = new ArrayList<>();
    for(String d: dataSources) {
      if(d == "structured_data"){
        Map<String, String> params = new HashMap<>();
        params.put("object_type", objectType);
        params.put("constraints", constraints[i]);
        params.put("filters", filters[i]);
        String json = jsonObjectMapper.writeValueAsString(params);
        System.out.println(String.format("composed json is %s", json));
        HttpClientResult res = HttpClientUtils.doPost(
            String.format(this.restApiBaseUrl + this.restUri.get("query"), objectType),
            new HashMap<String, String>() {{
              put("Content-Type", "application/json;charset=UTF-8");
              put("Accept", "application/json");
            }},
            json);
        if(res.getCode() != HttpStatus.SC_OK) {
          logger.error(String.format("Failed to complete query from rest api: {}. ", res.getContent()));
          throw new Exception(res.getContent());
        }

        JsonNode jsonNode = jsonObjectMapper.readTree(res.getContent());
        List<String> mediaUris = new ArrayList<>();
        List<String> mediaUrls = new ArrayList<>();

        if (jsonNode.isArray()) {
          for (final JsonNode oneNode : jsonNode) {
            if(oneNode.isArray()){
                Feature feature = new Feature();
                feature.setUnixTimestamp(oneNode.get(0).asLong());
                feature.setMediaUri(oneNode.get(1).asText());
                mediaUris.add(oneNode.get(1).asText());
                feature.setGeolocation(new GeoLocation(oneNode.get(2).asDouble(), oneNode.get(3).asDouble()));
                ret.add(feature);
            }
          }
        }

        try {
          MinioSDK.getPresignedObjectsUrl(this.minioClient, mediaUris, 7, mediaUrls);
          int j = 0;
          for(Feature f: ret){
            f.setMediaUrl(mediaUrls.get(j));
            j += 1;
          }
        } catch (Exception e) {
          logger.error(String.format("Failed to get meida urls from media uri: {}.", e));
        }

      }
      if(d == "feature_storage"){
        // todo
      }
      if(d == "clustered_feature_storage"){
        // todo
      }
      i += 1;
    }
    return ret;
  }

//  /**
//   * put unarchived feature vectors and their attributes to the specified data sources
//   * @param features
//   * @param dataSources
//   * @return objectType, featureVectorID as Map
//   */
//  public Map<String, String> putUnarchivedFeatures(Feature[] features, String[] dataSources) {
//    return null;
//  }

  /**
   * Get feature objects using the same mandatory attributes with the input feature for
   * big data engine
   * @param features
   * @param constraints
   * @param dataSources
   * @return the feature objects match the license place and other query conditions
   */
  public Map<String, List<Feature>> getDuplicateLicensePlate(Feature[] features, String constraints,
      String[] dataSources){
    return null;
  }

  /**
   * Get media uri and media url pairs thru cluster ids
   * @param objectType
   * @param clusterIDs
   * @param dataSource
   * @return media_uri and media_url pairs
   */
  public Map<String, String> getMediaUrlByClusterIDs(String objectType, String[] clusterIDs,
      String[] dataSource) throws Exception {
    if(objectType == null || objectType.isEmpty())
      throw new IllegalArgumentException("Object type cannot be null or empty.");
    Map<String, String> ret = new HashMap<>();
    for(String cvid: clusterIDs)
    {
      if(cvid == null || cvid.isEmpty() || cvid.length() != 32)
        throw new IllegalArgumentException(String.format("Invalid cluster id: ", cvid));
    }
    String params = jsonObjectMapper.writeValueAsString(clusterIDs);
    HttpClientResult res = HttpClientUtils.doPost(
        String.format(this.restApiBaseUrl + this.restUri.get("getBulkyArchiveUri"), objectType),
        new HashMap<String, String>() {{
          put("Content-Type", "application/json");
          put("Accept", "application/json");
        }},
        params);

    if(res.getCode() != HttpStatus.SC_OK) {
      logger.error(String.format("Failed to get archive from rest api: {}. ", res.getContent()));
      throw new Exception(res.getContent());
    }

    List<String> mediaUris = new ArrayList<>();
    List<String> mediaUrls = new ArrayList<>();
    try {
      JsonNode jsonNode = jsonObjectMapper.readTree(res.getContent());
      JsonNode colnames = jsonNode.get("colnames");
      int idx = 0;
      if (colnames.isArray()) {
        for (final JsonNode col : colnames) {
          if (col.asText().equals("media_uri"))
            break;
          idx += 1;
        }
      }

      JsonNode arrayNode = jsonNode.get("values");
      if (arrayNode.isArray()) {
        String mediaUri = null;
        for (JsonNode json : arrayNode) {
          mediaUri = json.get(idx).asText();
          mediaUris.add(mediaUri);
        }
      }
    } catch (Exception e) {
      logger.error(String.format("Failed to parse the get archive response: {}. ", e));
      throw new IOException(e);
    }

    try {
      MinioSDK.getPresignedObjectsUrl(this.minioClient, mediaUris, 7, mediaUrls);
      int j = 0;
      for(String url: mediaUrls){
        ret.put(clusterIDs[j], url);
        j += 1;
      }
    } catch (Exception e) {
      logger.error(String.format("Failed to get meida urls from media uri: {}.", e));
    }
    return ret;
  }

  /**
   * Get media uri and media url pairs thru feature vector ids
   * @param objectType
   * @param featureVectorIDs
   * @param dataSource
   * @return media_uri and media_url pairs
   */
  public Map<String, String> getMediaUrlByIDs(String objectType, String[] featureVectorIDs,
      String[] dataSource) throws Exception {
    if(objectType == null || objectType.isEmpty())
      throw new IllegalArgumentException("Object type cannot be null or empty.");
    Map<String, String> ret = new HashMap<>();
    for(String fvid: featureVectorIDs)
    {
      if(fvid == null || fvid.isEmpty() || fvid.length() != 36)
        throw new IllegalArgumentException(String.format("Invalid feature vector id: ", fvid));
    }
    String purl = String.format(this.restApiBaseUrl + this.restUri.get("getBulkyOccurrenceUri"), objectType);
    String params = jsonObjectMapper.writeValueAsString(featureVectorIDs);
    System.out.println(String.format("url is %s", purl));
    System.out.println(String.format("params is %s", params));
    HttpClientResult res = HttpClientUtils.doPost(
        String.format(this.restApiBaseUrl + this.restUri.get("getBulkyOccurrenceUri"), objectType),
        new HashMap<String, String>() {{
          put("Content-Type", "application/json");
          put("Accept", "application/json");
        }},
        params);

    if(res.getCode() != HttpStatus.SC_OK) {
      logger.error(String.format("Failed to get bulky occurrence from rest api: {}. ", res.getContent()));
      throw new Exception(res.getContent());
    }

    List<String> mediaUris = new ArrayList<>();
    List<String> mediaUrls = new ArrayList<>();

    try {
      JsonNode jsonNode = jsonObjectMapper.readTree(res.getContent());
      JsonNode colnames = jsonNode.get("colnames");
      int idx = 0;
      if (colnames.isArray()) {
        for (final JsonNode col : colnames) {
          if (col.asText().equals("media_uri"))
            break;
          idx += 1;
        }
      }

      JsonNode arrayNode = jsonNode.get("values");
      if (arrayNode.isArray()) {
        String mediaUri = null;
        for (JsonNode json : arrayNode) {
          mediaUri = json.get(idx).asText();
          mediaUris.add(mediaUri);
//          ret.put(mediaUri, "");
          System.out.println(mediaUri);
        }
      }
    } catch (Exception e) {
      logger.error(String.format("Failed to parse the get bulky occurrence response: %s.", e));
      throw new IOException(e);
    }

    try {
      MinioSDK.getPresignedObjectsUrl(this.minioClient, mediaUris, 7, mediaUrls);
      int j = 0;
      for(String url: mediaUrls){
        ret.put(featureVectorIDs[j], url);
        j += 1;
      }
    } catch (Exception e) {
      logger.error(String.format("Failed to get meida urls from media uri: %s.", e));
    }
    return ret;
  }

  /**
   * Get feature attributes and vectors by feature vector ids
   * @param objectType
   * @param featureVectorIDs
   * @param dataSource
   * @return feature object list
   */
  public List<Feature> getFeaturesByID(String objectType, String[] featureVectorIDs,
      String constraints, String[] dataSource) {
    return null;
  }

  /**
   * Get all districts existing in the capture source table, todo: check with GL on the object_type
   * in capture source
   * @return Provinces as String Array
   */
  public String[] getAllDistricts() throws Exception {
    HttpClientResult res = HttpClientUtils.doGet(
        this.restApiBaseUrl + this.restUri.get("getAllDistricts"));
    String[] ret = null;
    if(res.getCode() == HttpStatus.SC_OK){
      ret = jsonObjectMapper.readValue(res.getContent(), String[].class);
    }
    else{
      logger.error(String.format("Failed to get districts from rest api: {}. ", res.getContent()));
      throw new Exception(res.getContent());
    }
    return ret;
  }

  /**
   * Get cluster feature vector todo: to generalize the ClusterFeature class
   * @param objectType
   * @param dataSources
   * @param virtualIds
   * @return cluster feature objects
   */
  public List<ClusterFeature> getArchivedFeatures(String objectType, String[] dataSources,
      String[] virtualIds, String filters) throws Exception {
    List<ClusterFeature> cfs = new ArrayList<>();
    boolean createNewCf = true;
    for (String dataSource: dataSources) {
      if (dataSource == "structured_data") {
        if(virtualIds == null || virtualIds.length == 0)
          throw new IllegalArgumentException("Virtual ids cannot be null or empty.");
        String[] vids = Arrays.copyOf(virtualIds, virtualIds.length);
        for (int i=0; i < virtualIds.length; i++){
          vids[i] = String.format("%s%s%s", "'", vids[i], "'");
        }
        String where = String.join(", ", vids);
        where = String.format("archive.object_virtual_id in (%s);", where);
        Map<String, String> params = new HashMap<>();
        params.put("object_type", objectType);
        params.put("constraints", where);
        params.put("filters", filters);
        String json = jsonObjectMapper.writeValueAsString(params);
        HttpClientResult res = HttpClientUtils.doPost(
            String.format(this.restApiBaseUrl + this.restUri.get("query"), objectType),
            new HashMap<String, String>() {{
              put("Content-Type", "application/json;charset=UTF-8");
              put("Accept", "application/json");
            }},
            json);
        if(res.getCode() != HttpStatus.SC_OK) {
          logger.error(String.format("Failed to complete query from rest api: {}. ", res.getContent()));
          throw new Exception(res.getContent());
        }
        System.out.println(res.getContent());
        JsonNode jsonNode = jsonObjectMapper.readTree(res.getContent());
        Map attri = new HashMap();
        if (jsonNode.isArray()) {
          for (int i = 0; i < jsonNode.size(); i ++) {
            final JsonNode oneNode = jsonNode.get(i);
            if(oneNode.isArray()){
              ClusterFeature cf;
              if(createNewCf) {
                cf = new ClusterFeature();
              }
              else{
                cf = cfs.get(i);
              }
              cf.setLicensePlate(oneNode.get(0).asText());
              attri = new HashMap();
              attri.put("color", oneNode.get(1).asText());
              attri.put("brand", oneNode.get(2).asText());
              attri.put("model", oneNode.get(3).asText());
              cf.setAttributes(jsonObjectMapper.writeValueAsString(attri));

              if(createNewCf) {
                cfs.add(cf);
              }
            }
          }
          if(createNewCf){
            createNewCf = false;
          }
        }
      } else if (dataSource == "clustered_feature_storage") {
        String tableName = String.format("archived_%s_feature", objectType);
        for (int i = 0; i < virtualIds.length; i ++) {
          ClusterFeature cf;
          byte[] virtualIDBytes = virtualIds[i].getBytes(Charset.forName("UTF-8"));
          ClusterFeature e = null;
          try {
             e = client.get(tableName, virtualIDBytes, ClusterFeature.class);
          }
          catch(Exception ex){
            throw new IOException(String.format("Failed to connect to hbase %s", ex));
          }
          if(e == null) {
            throw new Exception(String.format("Unable to fetch data for %s index %s", dataSource,
                virtualIds[i]));
          }
          if(createNewCf) {
            cf = e;
          }
          else{
            cf = cfs.get(i);
            cf.setFeatureRK(e.getFeatureRK());
          }
          if(createNewCf) {
            cfs.add(cf);
          }
        }
        if(createNewCf){
          createNewCf = false;
        }
      }
    }
    return cfs; 
  }

  /**
   * Append feature vector ids by cluster id, update cluster center, add to vehicle table
   * @param objectType
   * @param featureVectorIDs
   * @param virtualID
   * @return statusCode
   */
  public int putArchivedFeatures(String objectType, String[][] featureVectorIDs,
      int[][] clusterCenter, String[] virtualID, String[] dataSource){
    return 0;
  }

  /**
   * Create virtual id for license plates, update feature vector ids and cluster center,
   * add to vehicle table
   * @param objectType
   * @param featureVectorIDs
   * @param clusterCenter
   * @param licensePlates
   * @param dataSource
   * @return statusCode
   */
  public int archiveFeatures(String objectType, String[][] featureVectorIDs,
      double[][] clusterCenter, String[] licensePlates, String[] dataSource){
    return 0;
  }

  /**
   * Connect behavior pattern with virtual id
   * @param behaviors
   * @param virtualID
   * @param objectType
   * @return
   */
  public int appendBehaviors(String[][] behaviors, String[] virtualID, String objectType) {
    return 0;
  }

  public List<Feature> queryBehavior(String dataSource, String[] indexes, String objectType,
      String constraint, String filter) throws Exception {
    if(indexes == null || indexes.length == 0)
      throw new IllegalArgumentException("Feature vector ids cannot be null or empty.");
    for (int i=0; i < indexes.length; i++){
      indexes[i] = String.format("%s%s%s", "'", indexes[i], "'");
    }
    String where = String.join(", ", indexes);
    where = String.format("%s AND occurrence.vehicle_feature_id in (%s);", constraint, where);
    return selectFeatures(objectType, new String[]{dataSource}, new String[]{where},
        new String[]{filter});
  }
}
