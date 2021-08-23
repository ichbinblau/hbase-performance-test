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

import java.io.Serializable;
import org.json.JSONException;
import org.json.JSONObject;


public class HttpClientResult implements Serializable {

  private static final long serialVersionUID = 3279262194164783061L;

  /**
   * status code
   */
  private int code;

  /**
   * response body
   */
  private String content;

  public HttpClientResult() {
  }

  public HttpClientResult(int code) {
    this.code = code;
  }

  public HttpClientResult(String content) {
    this.content = content;
  }

  public HttpClientResult(int code, String content) {
    this.code = code;
    this.content = content;
  }

  public int getCode() {
    return code;
  }

  public void setCode(int code) {
    this.code = code;
  }

  public String getContent() {
    return content;
  }

  public JSONObject getContentAsJson() {
    JSONObject jsonObject = null;
    try {
       jsonObject = new JSONObject(this.content);
    }catch (JSONException err){
      return null;
    }
    return jsonObject;
  }

  public void setContent(String content) {
    this.content = content;
  }

  public void setContent(JSONObject json) {
    this.content = json.toString();
  }

  @Override
  public String toString() {
    return "HttpClientResult [code=" + code + ", content=" + content + "]";
  }

}

