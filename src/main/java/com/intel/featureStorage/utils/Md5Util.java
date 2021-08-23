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

package com.intel.featureStorage.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * generate md5 hash
 */
public class Md5Util {

  private static char[] digit = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

  public static String getHash(String plaintext) {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] digest = md.digest(plaintext.getBytes());
      return byteToStr(digest);
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }

    return "";
  }

  private static String byteToStr(byte[] byteArray) {
    String rst = "";
    for (int i = 0; i < byteArray.length; i++) {
      rst += byteToHex(byteArray[i]);
    }
    return rst;
  }

  private static String byteToHex(byte b) {
    char[] tempArr = new char[2];
    tempArr[0] = digit[(b >>> 4) & 0X0F];
    tempArr[1] = digit[b & 0X0F];
    String s = new String(tempArr);
    return s;
  }
}
