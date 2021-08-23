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

import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.GetPresignedObjectUrlArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.RemoveBucketArgs;
import io.minio.RemoveObjectArgs;
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.UploadObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidBucketNameException;
import io.minio.errors.InvalidExpiresRangeException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.MinioException;
import io.minio.errors.RegionConflictException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.http.Method;
import io.minio.messages.Bucket;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.TimeUnit;
//import io.minio.DownloadObjectArgs;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.OutputStream;

/**
 * MinioSDK.
 */
public final class MinioSDK {

  static final long MAX_SIZE = 10485760;
  static final int ARRAY_SIZE = 1024;

  private MinioSDK() {
  }

  /**
   * Initial minio client.
   */
  public static MinioClient minioClient(String endpoint, String accessKey, String securityKey) {
    MinioClient client = MinioClient.builder().endpoint(endpoint)
        .credentials(accessKey, securityKey)
        .build();
    return client;
  }

  /**
   * Check if a bucket is exist.
   */
  public static int bucketExists(MinioClient client, String bucketName) throws ErrorResponseException,
      InsufficientDataException, InternalException, InvalidBucketNameException, InvalidKeyException,
      InvalidResponseException, IOException, NoSuchAlgorithmException, ServerException, XmlParserException {
    // Check whether 'my-bucketname' exists or not.
    boolean found =
        client.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
    if (found) {
      System.out.println(bucketName + " exists");
    } else {
      System.out.println(bucketName + " does not exist");
    }
    return 0;
  }

  /**
   * List Buckets.
   */
  public static int listBuckets(MinioClient client) throws MinioException, NoSuchAlgorithmException,
      IOException, InvalidKeyException {
    List<Bucket> bucketList = client.listBuckets();
    for (Bucket bucket : bucketList) {
      System.out.println(bucket.creationDate() + ", " + bucket.name());
    }
    return 0;
  }

  /**
   * Make a bucket.
   */
  public static int makeBucket(MinioClient client, String bucketName) throws ErrorResponseException,
      IllegalArgumentException, InsufficientDataException, InternalException, InvalidBucketNameException,
      InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
      RegionConflictException, ServerException, XmlParserException {
    // Create bucket with default region.
    client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
    return 0;
  }

  /**
   * Remove a bucket.
   */
  public static int removeBucket(MinioClient client, String bucketName) throws ErrorResponseException,
      IllegalArgumentException, InsufficientDataException, InternalException, InvalidBucketNameException,
      InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
      ServerException, XmlParserException {
    client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
    return 0;
  }

  /**
   * Download a onbject.
   */
    /*public static void downloadObject(MinioClient client, String bucketName, String objectName, String fileName)
        throws ErrorResponseException, IllegalArgumentException,
        InsufficientDataException, InternalException, InvalidBucketNameException, InvalidKeyException,
        InvalidResponseException, IOException, NoSuchAlgorithmException, ServerException, XmlParserException {
        client.downloadObject(DownloadObjectArgs.builder().bucket(bucketName).object(objectName)
            .fileName(fileName)
            .build());
    }*/

  /**
   * Gets data of an object, and save to the specified file.
   */
  public static int getObject(MinioClient client, String bucketName, String objectName, String fileName)
      throws ErrorResponseException, IllegalArgumentException, InsufficientDataException, InternalException,
      InvalidBucketNameException, InvalidKeyException, InvalidResponseException, IOException,
      NoSuchAlgorithmException, ServerException, XmlParserException {
    // get object given the bucket and object name
    try (InputStream stream = client.getObject(
        GetObjectArgs.builder()
            .bucket(bucketName)
            .object(objectName)
            .build());
        OutputStream  outstream = new FileOutputStream(fileName)) {
      byte[] buffer = new byte[ARRAY_SIZE];
      int length = 0;
      while ((length = stream.read(buffer)) != -1) {
        outstream.write(buffer, 0, length);
      }
      System.out.println("Have saved to file: " + fileName);
      return 0;
    }
  }

  /**
   * Get an object url.
   */
  public static String getObjectUrl(MinioClient client, String bucketName, String objectName)
      throws ErrorResponseException, IllegalArgumentException, InsufficientDataException, InternalException,
      InvalidBucketNameException, InvalidKeyException, InvalidResponseException, IOException,
      NoSuchAlgorithmException, ServerException, XmlParserException {
    String url = client.getObjectUrl(bucketName, objectName);
    return url;
  }

  /**
   * Get an object url.
   */
  public static String getPresignedObjectUrl(MinioClient client, String bucketName, String objectName, int day)
      throws ErrorResponseException, IllegalArgumentException, InsufficientDataException, InternalException,
      InvalidBucketNameException, InvalidExpiresRangeException, InvalidKeyException, InvalidResponseException,
      IOException, NoSuchAlgorithmException, ServerException, XmlParserException {
    String url = client.getPresignedObjectUrl(
        GetPresignedObjectUrlArgs.builder()
            .method(Method.GET)
            .bucket(bucketName)
            .object(objectName)
            .expiry(day, TimeUnit.DAYS)
            .build());
    return url;
  }

  /**
   * Get objects url.
   */
  public static int getPresignedObjectsUrl(MinioClient client, List<String> uris, int day, List<String> urls)
      throws ErrorResponseException, IllegalArgumentException, InsufficientDataException, InternalException,
      InvalidBucketNameException, InvalidExpiresRangeException, InvalidKeyException, InvalidResponseException,
      IOException, NoSuchAlgorithmException, ServerException, XmlParserException {
    String url;
    for (int i = 0; i < uris.size(); i++) {
      String bucketName = uris.get(i).substring(0,4) + uris.get(i).substring(4,8);
      String objecName = uris.get(i);
      url = getPresignedObjectUrl(client, bucketName, objecName, day);
      urls.add(url);
    }
    return 0;
  }

  /**
   * Lists objects information recursively.
   */
  public static Iterable<Result<Item>> listObjects(MinioClient client, String bucketName)
      throws XmlParserException {
    // Lists objects information.
    Iterable<Result<Item>> results = client.listObjects(
        ListObjectsArgs.builder().bucket(bucketName).recursive(true).build());
    return results;
  }

  /**
   * Uploads given stream as an object in bucket.
   */
  public static ObjectWriteResponse putObject(MinioClient client, String bucketName, String objectName,
      InputStream inputStream)
      throws ErrorResponseException, IllegalArgumentException, InsufficientDataException, InternalException,
      InvalidBucketNameException, InvalidKeyException, InvalidResponseException, IOException,
      NoSuchAlgorithmException, ServerException, XmlParserException {
    ObjectWriteResponse owr = client.putObject(
        PutObjectArgs.builder().bucket(bucketName).object(objectName).stream(
            inputStream, -1, MAX_SIZE).build());
    return owr;
  }

  /**
   * Remove an object from a bucket.
   */
  public static int removeObject(MinioClient client, String bucketName, String objectName)
      throws ErrorResponseException, IllegalArgumentException, InsufficientDataException,
      InternalException, InvalidBucketNameException, InvalidKeyException, InvalidResponseException,
      IOException, NoSuchAlgorithmException, ServerException, XmlParserException {
    client.removeObject(
        RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build());
    return 0;
  }

  /**
   * Remove objects from a bucket.
   */
  public static Iterable<Result<DeleteError>> removeObjects(MinioClient client, String bucketName,
      List<DeleteObject> objects) {
    Iterable<Result<DeleteError>> results = client.removeObjects(
        RemoveObjectsArgs.builder().bucket(bucketName).objects(objects).build());
    return results;
  }

  /**
   * Uploads data from a file to an object(Replace data).
   */
  public static int uploadObject(MinioClient client, String bucketName, String objectName, String fileName)
      throws ErrorResponseException, IllegalArgumentException, InsufficientDataException,
      InternalException, InvalidBucketNameException, InvalidKeyException, InvalidResponseException,
      IOException, NoSuchAlgorithmException, ServerException, XmlParserException {
    client.uploadObject(
        UploadObjectArgs.builder()
            .bucket(bucketName).object(objectName).filename(fileName).build());
    return 0;
  }

}


