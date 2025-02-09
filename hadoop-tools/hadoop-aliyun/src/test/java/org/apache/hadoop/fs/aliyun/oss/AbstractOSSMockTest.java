/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.model.GenericRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.ListObjectsV2Request;
import com.aliyun.oss.model.ListObjectsV2Result;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import java.net.URI;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.SimpleTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.aliyun.oss.Constants.ACCESS_KEY_ID;
import static org.apache.hadoop.fs.aliyun.oss.Constants.ACCESS_KEY_SECRET;
import static org.apache.hadoop.fs.aliyun.oss.Constants.ENDPOINT_KEY;
import static org.apache.hadoop.fs.aliyun.oss.Constants.FS_OSS;
import static org.apache.hadoop.fs.aliyun.oss.Constants.OSS_CLIENT_FACTORY_IMPL;
import static org.mockito.ArgumentMatchers.argThat;

public abstract class AbstractOSSMockTest {
  protected int bufferSize = 512;
  protected short replication = 1;
  protected long blockSize = 512;
  protected static final String BUCKET = "mock-bucket-oss";
  protected AliyunOSSFileSystem fs;
  protected OSSClient ossClient;
  protected static final String ENDPOINT = "oss-us-west-1.aliyuncs.com";

  protected static final int LIST_V1 = 1;
  protected static final int LIST_V2 = 2;

  protected static final ServiceException NOT_FOUND = new OSSException("Not Found", OSSErrorCode.NO_SUCH_KEY,
      "requestIdtest", "hostIdtest",
      "headertest", "resourceTypetest", "methodTest");

  protected Configuration conf;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setup() throws Exception {
    createConfiguration();
    conf.set(ENDPOINT_KEY, ENDPOINT, "OSS UT TEST");
    conf.set(ACCESS_KEY_ID, "testaccesskey", "OSS UT TEST");
    conf.set(ACCESS_KEY_SECRET, "testSecretkey", "OSS UT TEST");
    initFs();
  }

  @After
  public void cleanup() throws Exception {
    fs.close();
    fs = null;
    ossClient = null;
  }

  public void createConfiguration() {
    this.conf = new Configuration();
    conf.setClass(OSS_CLIENT_FACTORY_IMPL, MockOSSClientFactory.class, OSSClientFactory.class);
  }

  protected void initFs() throws Exception {
    fs = new AliyunOSSFileSystem();
    URI uri = URI.create(FS_OSS + "://" + BUCKET);
    fs.initialize(uri, conf);
    ossClient = fs.getStore().getOSSClient();
  }

  protected void resetFileSystem() throws Exception {
    cleanup();
    initFs();
  }

  protected Configuration getConf() {
    return conf;
  }

  protected OSSClient getOSSClient() {
    return ossClient;
  }

  protected void setMockList(int listVersion, String bucket, String delimiter, boolean isTruncated,
      String prefix, int maxKeys, List<String> keys, List<String> commonPrefixes) throws Exception {
    if (listVersion == 1) {
      ObjectListing listV1Res = new ObjectListing();
      generatListResult(listV1Res, BUCKET, delimiter, isTruncated, prefix, 100, keys,
          commonPrefixes);
      Mockito.when(ossClient.listObjects(argThat(requestListV1Matcher(bucket, prefix))))
          .thenReturn(listV1Res);
      return;
    } else if (listVersion == 2) {
      ListObjectsV2Result listV2Res = new ListObjectsV2Result();
      generatListResult(listV2Res, bucket, delimiter, isTruncated, prefix, 100, keys,
          commonPrefixes);
      Mockito.when(ossClient.listObjectsV2(argThat(requestListV2Matcher(bucket, prefix))))
          .thenReturn(listV2Res);
      return;
    }

    throw new RuntimeException("not support list version" + listVersion);
  }

  protected ArgumentMatcher<GenericRequest> matchGetMetadataRequest(String bucket, String key) {
    return request -> request != null && request.getBucketName().equals(bucket) && request.getKey()
        .equals(key);
  }

  private ListObjectsV2Result getObjectAsListObjectsV2Result(Object obj) {
    if (obj instanceof ListObjectsV2Result) {
      return (ListObjectsV2Result) obj;
    } else {
      throw new IllegalArgumentException("Object is not an instance of ListObjectsV2Result");
    }
  }

  private ObjectListing getObjectAsObjectListing(Object obj) {
    if (obj instanceof ObjectListing) {
      return (ObjectListing) obj;
    } else {
      throw new IllegalArgumentException("Object is not an instance of ObjectListing");
    }
  }

  private void generatListResult(Object targetType, String bucket, String delimiter,
      boolean isTruncated, String prefix, int maxKeys, List<String> keys,
      List<String> commonPrefixes) throws ParseException {
    if (targetType instanceof ListObjectsV2Result) {
      ListObjectsV2Result listRes = getObjectAsListObjectsV2Result(targetType);
      listRes.setBucketName(bucket);
      listRes.setTruncated(isTruncated);
      listRes.setPrefix(prefix);
      listRes.setMaxKeys(maxKeys);
      listRes.setContinuationToken("token1");
      listRes.setNextContinuationToken("token2");
      listRes.setEncodingType("url");
      listRes.setDelimiter(delimiter);

      for (String key : keys) {
        OSSObjectSummary ossObjectSummary = new OSSObjectSummary();
        ossObjectSummary.setKey(key);
        ossObjectSummary.setETag("etagxxxxxx");
        ossObjectSummary.setLastModified(parseIso8601Date("2020-05-18T05:45:43.000Z"));
        ossObjectSummary.setSize(0);
        ossObjectSummary.setStorageClass("STANDARD");
        ossObjectSummary.setBucketName(bucket);
        listRes.addObjectSummary(ossObjectSummary);
      }

      for (String commomPrefixItem : commonPrefixes) {
        listRes.addCommonPrefix(commomPrefixItem);
      }
      return;
    } else if (targetType instanceof ObjectListing) {
      ObjectListing listRes = getObjectAsObjectListing(targetType);
      listRes.setBucketName(bucket);

      for (String key : keys) {
        OSSObjectSummary ossObjectSummary = new OSSObjectSummary();
        ossObjectSummary.setKey(key);
        ossObjectSummary.setSize(0);
        ossObjectSummary.setStorageClass("STANDARD");
        listRes.addObjectSummary(ossObjectSummary);
      }

      for (String commomPrefixItem : commonPrefixes) {
        listRes.addCommonPrefix(commomPrefixItem);
      }
      return;
    } else {
      throw new IllegalArgumentException(
          "Unsupported type for D: " + targetType.getClass().getName());
    }
  }

  protected static Date parseIso8601Date(String dateString) throws ParseException {
    try {
      return getIso8601DateFormat().parse(dateString);
    } catch (ParseException e) {
      return getAlternativeIso8601DateFormat().parse(dateString);
    }
  }

  protected static DateFormat getIso8601DateFormat() {
    String ISO8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    SimpleDateFormat df = new SimpleDateFormat(ISO8601_DATE_FORMAT, Locale.US);
    df.setTimeZone(new SimpleTimeZone(0, "GMT"));
    return df;
  }

  protected static DateFormat getAlternativeIso8601DateFormat() {
    String ALTERNATIVE_ISO8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    SimpleDateFormat df = new SimpleDateFormat(ALTERNATIVE_ISO8601_DATE_FORMAT, Locale.US);
    df.setTimeZone(new SimpleTimeZone(0, "GMT"));
    return df;
  }

  protected ArgumentMatcher<ListObjectsRequest> requestListV1Matcher(String bucket, String prefix) {
    return request -> request != null && request.getBucketName().equals(bucket)
        && request.getPrefix().equals(prefix);
  }

  protected ArgumentMatcher<ListObjectsV2Request> requestListV2Matcher(String bucket,
      String prefix) {
    return request -> request != null && request.getBucketName().equals(bucket)
        && request.getPrefix().equals(prefix);
  }

  protected ArgumentMatcher<GenericRequest> requestMatcher(String bucket, String key) {
    return new ArgumentMatcher<GenericRequest>() {
      @Override
      public boolean matches(GenericRequest request) {
        return request != null && request.getBucketName().equals(bucket) && request.getKey()
            .equals(key);
      }
    };
  }

  protected void checkGetObjectMetadataCalled(int times, ArgumentMatcher<GenericRequest> matcher) {
    Mockito.verify(ossClient, Mockito.times(times)).getObjectMetadata(argThat(matcher));
  }

  protected void checkListCalled(int listVersion, int expectTimes, String bucket, String prefix) {
    if (listVersion == LIST_V1) {
      Mockito.verify(ossClient, Mockito.times(expectTimes))
          .listObjects(argThat(requestListV1Matcher(bucket, prefix)));
      return;
    } else if (listVersion == LIST_V2) {
      Mockito.verify(ossClient, Mockito.times(expectTimes))
          .listObjectsV2(argThat(requestListV2Matcher(bucket, prefix)));
      return;
    }
    throw new RuntimeException("not support list version" + listVersion);
  }
}
