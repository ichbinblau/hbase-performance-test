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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;


public class HttpClientUtils {

  // data encoding
  private static final String ENCODING = "UTF-8";

  // connection timeout in million seconds
  private static final int CONNECT_TIMEOUT = 6000;

  // response timeout in million seconds
  private static final int SOCKET_TIMEOUT = 6000;

  public static HttpClientResult doGet(String url) throws Exception {
    return doGet(url, null, null);
  }

  public static HttpClientResult doGet(String url, Map<String, String> params) throws Exception {
    return doGet(url, null, params);
  }

  public static HttpClientResult doGet(String url, Map<String, String> headers, Map<String, String> params) throws Exception {
    CloseableHttpClient httpClient = HttpClients.createDefault();

    URIBuilder uriBuilder = new URIBuilder(url);
    if (params != null) {
      Set<Entry<String, String>> entrySet = params.entrySet();
      for (Entry<String, String> entry : entrySet) {
        uriBuilder.setParameter(entry.getKey(), entry.getValue());
      }
    }

    HttpGet httpGet = new HttpGet(uriBuilder.build());
    RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
    httpGet.setConfig(requestConfig);

    packageHeader(headers, httpGet);

    // 创建httpResponse对象
    CloseableHttpResponse httpResponse = null;

    try {
      return getHttpClientResult(httpResponse, httpClient, httpGet);
    } finally {
      release(httpResponse, httpClient);
    }
  }

  public static HttpClientResult doPost(String url) throws Exception {
    return doPost(url, null, null);
  }

  public static HttpClientResult doPost(String url, String params) throws Exception {
    return doPost(url, null, params);
  }

  public static HttpClientResult doPost(String url, Map<String, String> headers, String params) throws Exception {
    // create httpClient object
    CloseableHttpClient httpClient = HttpClients.createDefault();

    // create http object
    HttpPost httpPost = new HttpPost(url);

    RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
    httpPost.setConfig(requestConfig);

    packageHeader(headers, httpPost);

    packageParam(params, httpPost);

    CloseableHttpResponse httpResponse = null;

    try {
      return getHttpClientResult(httpResponse, httpClient, httpPost);
    } finally {
      release(httpResponse, httpClient);
    }
  }

  public static HttpClientResult doPut(String url) throws Exception {
    return doPut(url);
  }

  public static HttpClientResult doPut(String url, String params) throws Exception {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    HttpPut httpPut = new HttpPut(url);
    RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
    httpPut.setConfig(requestConfig);

    packageParam(params, httpPut);

    CloseableHttpResponse httpResponse = null;

    try {
      return getHttpClientResult(httpResponse, httpClient, httpPut);
    } finally {
      release(httpResponse, httpClient);
    }
  }

  public static HttpClientResult doDelete(String url) throws Exception {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    HttpDelete httpDelete = new HttpDelete(url);
    RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
    httpDelete.setConfig(requestConfig);

    CloseableHttpResponse httpResponse = null;
    try {
      return getHttpClientResult(httpResponse, httpClient, httpDelete);
    } finally {
      release(httpResponse, httpClient);
    }
  }

  public static HttpClientResult doDelete(String url, String params) throws Exception {
    if (params == null) {
      params = new String();
    }

    return doPost(url, params);
  }

  public static void packageHeader(Map<String, String> params, HttpRequestBase httpMethod) {
    if (params != null) {
      Set<Entry<String, String>> entrySet = params.entrySet();
      for (Entry<String, String> entry : entrySet) {
        httpMethod.setHeader(entry.getKey(), entry.getValue());
      }
    }
  }

  public static void packageParam(String params, HttpEntityEnclosingRequestBase httpMethod)
      throws UnsupportedEncodingException {
    if (params != null) {

      StringEntity entity = new StringEntity(params, "UTF-8");
      httpMethod.setEntity(entity);
    }
  }

  public static HttpClientResult getHttpClientResult(CloseableHttpResponse httpResponse,
      CloseableHttpClient httpClient, HttpRequestBase httpMethod) throws Exception {
    httpResponse = httpClient.execute(httpMethod);

    if (httpResponse != null && httpResponse.getStatusLine() != null) {
      String content = "";
      if (httpResponse.getEntity() != null) {
        content = EntityUtils.toString(httpResponse.getEntity(), ENCODING);

      }
      return new HttpClientResult(httpResponse.getStatusLine().getStatusCode(), content);
    }
    return new HttpClientResult(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }

  public static void release(CloseableHttpResponse httpResponse, CloseableHttpClient httpClient)
      throws IOException {
    if (httpResponse != null) {
      httpResponse.close();
    }
    if (httpClient != null) {
      httpClient.close();
    }
  }
}
