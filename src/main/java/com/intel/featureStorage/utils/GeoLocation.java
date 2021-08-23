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

/**
 * This class stores information about a location on Earth.  Locations are
 *  specified using latitude and longitude.  The class includes a method for
 *  computing the distance between two locations.
 */
public class GeoLocation {
  public static final double RADIUS = 6371.0088;  // Earth radius in miles

  private double latitude;
  private double longitude;

  // constructs a geo location object with given latitude and longitude
  public GeoLocation(double theLatitude, double theLongitude) {
    latitude = theLatitude;
    longitude = theLongitude;
  }

  // returns the latitude of this geo location
  public double getLatitude() {
    return latitude;
  }

  // returns the longitude of this geo location
  public double getLongitude() {
    return longitude;
  }

  // returns a string representation of this geo location
  public String toString() {
    return "latitude: " + latitude + ", longitude: " + longitude;
  }

  // returns the distance in kilo-meters between this geo location and the given
  // other geo location
  public double distanceFrom(GeoLocation other) {
    double lat1 = Math.toRadians(latitude);
    double long1 = Math.toRadians(longitude);
    double lat2 = Math.toRadians(other.latitude);
    double long2 = Math.toRadians(other.longitude);
    // apply the spherical law of cosines with a triangle composed of the
    // two locations and the north pole
    double theCos = Math.sin(lat1) * Math.sin(lat2) +
        Math.cos(lat1) * Math.cos(lat2) * Math.cos(long1 - long2);
    double arcLength = Math.acos(theCos);
    return arcLength * RADIUS;
  }
}
