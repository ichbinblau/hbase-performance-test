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

import com.flipkart.hbaseobjectmapper.HBObjectMapper;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.intel.featureStorage.utils.HbaseConnectionHelper;
import com.intel.featureStorage.utils.RandomGUID;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.codec.DecoderException;
//import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseClient implements FeatureStorageClient, Closeable {
  private static Logger logger = LoggerFactory.getLogger(HbaseClient.class);

  // default hbase column family name
  private final String[] columnFamilies = {"t",};

//  private FeatureStorageProperties properties;

  private static String regionNum;

  private final HBObjectMapper hbObjectMapper = new HBObjectMapper();

//  @PostConstruct
  public HbaseClient() {
    regionNum = FeatureStorageConfig.getConfigValues("region.number");
    logger.info("Region num: {}", regionNum);
  }

//  @Autowired
//  public void setFeatureStorageProperties(FeatureStorageProperties property){
//    this.properties = property;
//  }

  @Override
  public void close() throws IOException {
    HbaseConnectionHelper.closeConnection();
  }

  @Override
  public void createTable(String tableName) throws IOException{
    HbaseConnectionHelper.createTable(tableName, this.columnFamilies);
  }

  @Override
  public void dropTable(String tableName) throws IOException{
    HbaseConnectionHelper.dropTable(tableName);
  }

  @Override
  public boolean hasTable(String tableName) throws IOException{
    return HbaseConnectionHelper.hasTable(tableName);
  }

  @Override
  public <T> long put(String tableName, T put)  throws IOException{
    return put(tableName, Arrays.asList(put));
  }

  @Override
  public <T> long put(String tableName, List<T> puts) throws IOException{
    return HbaseConnectionHelper.putBatch(tableName, (List<Put>) puts);
  }

  @Override
  public <T extends HBRecord> T get(String tableName, byte[] rowKey, Class<T> clazz)
      throws IOException, DecoderException {
    List<T> lf = get(tableName, Arrays.asList(rowKey), clazz);
    if(lf.isEmpty())
      return null;
    else
      return lf.get(0);
  }

  @Override
  public <T extends HBRecord> List<T> get(String tableName, List<byte[]> rowKeys, Class<T> clazz)
      throws IOException, DecoderException {
    List<T> fs = new ArrayList<>();
    Result[] ret = HbaseConnectionHelper.get(tableName, rowKeys);
    int i = 0;
    for(Result r: ret){
      fs.add(map2Object(rowKeys.get(i), r, clazz));
      i += 1;
    }
    if (fs.isEmpty())
      return null;
    else
      return fs;
  }

  @Override
  public void delete(String tableName, String rowKey) throws IOException {
    delete(tableName, new String[]{rowKey});
  }

  @Override
  public void delete(String tableName, String[] rowKeys) throws IOException {
    HbaseConnectionHelper.delete(tableName, rowKeys);
  }

  @Override
  public <T> List<T> scan(String tableName, Object startRowKey, Object stopRowKey)
      throws IOException {
    // not implemented yet
    return null;
  }

  @Override
  public <T extends HBRecord> T map2Object(byte[] rowKey, Result result, Class<T> clazz) {
    T e = hbObjectMapper.readValue(new ImmutableBytesWritable(rowKey), result, clazz);
    return e;
  }

  public static byte[] genRowKey(long unixTimestamp) {
    short num;
    try {
      num = Short.parseShort(regionNum);
    }catch(Exception e){
      num = 1; // use default region number if it does not exist
    }
    short salt = (short)(unixTimestamp % num);
    String uuid = RandomGUID.generate().substring(0, 16);
    // 2 bytes + 8 bytes + 16 bytes
    return Bytes.add(Bytes.toBytes(salt), Bytes.toBytes(unixTimestamp), Bytes.toBytes(uuid));
  }
}
