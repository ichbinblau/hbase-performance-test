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

import com.flipkart.hbaseobjectmapper.HBRecord;
import java.io.IOException;
import java.util.List;
import javax.naming.InvalidNameException;
import org.apache.commons.codec.DecoderException;
import org.apache.hadoop.hbase.client.Result;

public interface FeatureStorageClient {

  void createTable(String tableName) throws IOException;

  boolean hasTable(String tableName) throws IOException;

  void dropTable(String tableName) throws IOException;

  <T> long put(String tableName, T t)  throws IOException;

  <T> long put(String tableName, List<T> puts) throws IOException;

  <T extends HBRecord> T get(String tableName, byte[] rowKey, Class<T> clazz)
      throws IOException, InvalidNameException, DecoderException;

  <T extends HBRecord> List<T> get(String tableName, List<byte[]> rowKeys, Class<T> clazz)
      throws IOException, InvalidNameException, DecoderException;

  void delete(String tableName, String rowKey) throws IOException;

  void delete(String tableName, String[] rowKey) throws IOException;

  // convert hbase result to specified class
  <T extends HBRecord> T map2Object(byte[] rowKey, Result result, Class<T> clazz)
      throws InvalidNameException, DecoderException;

  // todo: scan, timestamp range scan
  <T> List<T> scan(String tableName, Object startrowkey, Object stoprowkey) throws IOException;
}
