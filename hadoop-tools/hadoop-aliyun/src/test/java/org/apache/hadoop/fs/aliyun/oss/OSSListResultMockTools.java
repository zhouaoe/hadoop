/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.SimpleTimeZone;

import java.io.IOException;
import java.net.URI;
import java.security.Permissions;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.SimpleTimeZone;
import java.util.logging.Logger;

import javax.security.auth.login.Configuration;

import java.util.Date;

import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.aliyun.oss.OSS;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.mockito.Mockito;
import org.mockito.ArgumentMatcher;
import static org.mockito.ArgumentMatchers.argThat;

import com.aliyun.oss.model.HeadObjectRequest;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.GenericRequest;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ListObjectsV2Request;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.ListObjectsV2Result;
import com.aliyun.oss.model.ObjectMetadata;

import static org.apache.hadoop.fs.aliyun.oss.Constants.*;

public class OSSListResultMockTools<R extends GenericRequest> {
    // private int listVersion;

    // OSSListResultMockTools(int listVersion) {
    // this.listVersion = listVersion;
    // }

    // public ArgumentMatcher<R extends GenericRequest> matchGenericRequest(
    //         String bucket, String prefix) {
    //     System.out.println(" bucket:" + bucket + " prefix:" + prefix);
    //     return (R request) -> {
    //         if (request instanceof ListObjectsV2Request) {
    //             ListObjectsV2Request v2Request = (ListObjectsV2Request) request;
    //             System.out.println("v2 request");
    //             return v2Request.getBucketName().equals(bucket) && v2Request.getPrefix().equals(prefix);
    //         } else if (request instanceof ListObjectsRequest) {
    //             ListObjectsRequest v1Request = (ListObjectsRequest) request;
    //             System.out.println("v1 request is not null");
    //             return v1Request.getBucketName().equals(bucket)
    //                     && v1Request.getPrefix().equals(prefix);
    //         } else {
    //             return false;
    //         }
    //     };
    // }

    // public ArgumentMatcher<ListObjectsRequest> matchListV1Request(
    // String bucket, String prefix) {
    // System.out.println("matchListV1Request bucket:" + bucket + " prefix:" +
    // prefix);
    // return (ListObjectsRequest request) -> {
    // if (request != null) {
    // System.out.println("matchListV2Request request is not null");
    // System.out.println("matchListV2Request request.getBucketName():" +
    // request.getBucketName());
    // System.out.println("matchListV2Request request.getPrefix():" +
    // request.getPrefix());
    // }
    // return request != null
    // && request.getBucketName().equals(bucket)
    // && request.getPrefix().equals(prefix);
    // };
    // }

    // ObjectListing generatListV1Result(String bucket, String delimiter, boolean
    // isTruncated, String Prefix,
    // int maxKeys, List<String> keys, List<String> commonPrefixes) {
    // D listRes = new D();
    // listRes.setBucketName(BUCKET);

    // for (String key : keys) {
    // OSSObjectSummary ossObjectSummary = new OSSObjectSummary();
    // ossObjectSummary.setKey(key);
    // ossObjectSummary.setSize(0);
    // ossObjectSummary.setStorageClass("STANDARD");
    // listRes.addObjectSummary(ossObjectSummary);
    // }

    // for (String prefix : commonPrefixes) {
    // listRes.addCommonPrefix(prefix);
    // }
    // return listRes;
    // }

    // private ListObjectsV2Result getObjectAsListObjectsV2Result(Object obj) {
    //     return Optional.ofNullable(obj)
    //             .filter(ListObjectsV2Result.class::isInstance)
    //             .map(ListObjectsV2Result.class::cast)
    //             .orElseThrow(() -> new IllegalArgumentException("Object is not an instance of ListObjectsV2Result"));
    // }
    
    // private ObjectListing getObjectAsObjectListing(Object obj) {
    //     return Optional.ofNullable(obj)
    //             .filter(ObjectListing.class::isInstance)
    //             .map(ObjectListing.class::cast)
    //             .orElseThrow(() -> new IllegalArgumentException("Object is not an instance of ObjectListing"));
    // }

    

}
