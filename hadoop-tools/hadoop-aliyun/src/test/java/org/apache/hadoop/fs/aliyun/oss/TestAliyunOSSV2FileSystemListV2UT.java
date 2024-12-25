package org.apache.hadoop.fs.aliyun.oss;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
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
import java.util.Date;

import org.apache.hadoop.fs.aliyun.oss.AliyunOSSV2FileSystem;
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


public class TestAliyunOSSV2FileSystemListV2UT extends AbstractOSSMockTest {
  // private static final Logger LOG =
      // LoggerFactory.getLogger(TestAliyunOSSV2FileSystemUT.class);

    @Test
    public void testCreateWhenFileDoesNotExistWith() throws Exception {
        System.out.println("testCreate");
        Path path = new Path("/a.txt");
        String key = path.toUri().getPath().substring(1);
        String keyDir= key+"/";
        System.out.println("testCreate key:" + key);
        FsPermission permission = new FsPermission((short) 0777);
        boolean overwrite = true;
        int bufferSize = 512;
        short replication = 1;
        long blockSize = 512;
        Progressable progress = null;

        OSSClient ossMockClient = getOSSClient();
        Mockito.when(ossClient.getObjectMetadata(argThat(correctGetMetadataRequest(BUCKET, keyDir)))).thenThrow(NOT_FOUND);

        ListObjectsV2Result listV2Res = generatListV2Result(BUCKET, "", false, keyDir, 100, new ArrayList<>(), new ArrayList<>());
        //return a empty result
        Mockito.when(ossClient.listObjectsV2(argThat(matchListV2Request(BUCKET, keyDir)))).thenReturn(listV2Res);
        FSDataOutputStream outstream = fs.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
        System.out.println("success testCreate"); 
        assertTrue(outstream!=null);
    }

    public void testCreateWhenFileDoesExist() throws Exception {
      System.out.println("testCreate");
      Path path = new Path("/a.txt");
      String key = path.toUri().getPath().substring(1);
      String keyDir= key+"/";
      System.out.println("testCreate key:" + key);
      FsPermission permission = new FsPermission((short) 0777);
      boolean overwrite = true;
      int bufferSize = 512;
      short replication = 1;
      long blockSize = 512;
      Progressable progress = null;

      ObjectMetadata meta = new ObjectMetadata();
      meta.setLastModified(parseIso8601Date("2017-07-07T08:08:08.008Z"));
      OSSClient ossMockClient = getOSSClient();
      meta.setLastModified(parseIso8601Date("2017-07-07T08:08:08.008Z"));
      Mockito.when(ossClient.getObjectMetadata(argThat(new ArgumentMatcher<GenericRequest>() {
        @Override
        public boolean matches(GenericRequest request) {
            return correctGetMetadataRequest(BUCKET, key).matches(request);
        }
    }))).thenReturn(meta);

      ListObjectsV2Result listV2Res = generatListV2Result(BUCKET, "", false, keyDir, 100, new ArrayList<>(), new ArrayList<>());
      //return a empty result
      Mockito.when(ossClient.listObjectsV2(argThat(matchListV2Request(BUCKET, keyDir)))).thenReturn(listV2Res);
      FSDataOutputStream outstream = fs.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
      System.out.println("success testCreate"); 
      assertTrue(outstream!=null);
  }


    private ArgumentMatcher<GenericRequest> correctGetMetadataRequest(
      String bucket, String key) {
      return request -> request != null
              && request.getBucketName().equals(bucket)
              && request.getKey().equals(key);
      }

    //   private ArgumentMatcher<ListObjectsRequest> matchListRequest(
    //     String bucket, String key) {
    //   return (ListObjectsRequest request) -> {
    //     return request != null
    //         && request.getBucketName().equals(bucket)
    //         && request.getKey().equals(key);
    //   };
    // }

    private ArgumentMatcher<ListObjectsV2Request> matchListV2Request(
      String bucket, String prefix) {
        System.out.println("matchListV2Request bucket:" + bucket + " prefix:" + prefix);
    return (ListObjectsV2Request request) -> {
      if(request != null)
      {
        System.out.println("matchListV2Request request is not null");
        System.out.println("matchListV2Request request.getBucketName():" + request.getBucketName());
        System.out.println("matchListV2Request request.getPrefix():" + request.getPrefix());
      }
      return request != null
          && request.getBucketName().equals(bucket)
          && request.getPrefix().equals(prefix);
    };
  }

  // ObjectListing generatListV1Result()
  // {
  //   ObjectListing listRes= new ObjectListing();
  //   listRes.setBucketName(BUCKET);

  //   List<OSSObjectSummary> objectSummaries = new ArrayList<OSSObjectSummary>();
  //   listRes.setObjectSummaries(objectSummaries);
  //   OSSObjectSummary ossObjectSummary = new OSSObjectSummary();
  //   ossObjectSummary.setKey(key);
  //   ossObjectSummary.setSize(0);
  //   ossObjectSummary.setStorageClass("STANDARD");
  //   objectSummaries.add(ossObjectSummary);
  //   List<String> commonPrefixes = new ArrayList<String>();
  //   listRes.setCommonPrefixes(commonPrefixes);
  //   commonPrefixes.add("prefix");
  //   return listRes;
  // }
  
  ListObjectsV2Result generatListV2Result(String bucket,String delimiter, boolean isTruncated, String Prefix, int maxKeys,
      List<String> keys, List<String> commonPrefixes) throws ParseException {
    ListObjectsV2Result listRes= new ListObjectsV2Result();
    listRes.setBucketName(bucket);
    listRes.setTruncated(isTruncated);
    listRes.setPrefix(Prefix);
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

    for (String prefix : commonPrefixes) {
      listRes.addCommonPrefix(prefix);
    }
    return listRes;
  }

      public static Date parseIso8601Date(String dateString) throws ParseException {
        try {
            return getIso8601DateFormat().parse(dateString);
        } catch (ParseException e) {
            return getAlternativeIso8601DateFormat().parse(dateString);
        }
    }

    private static DateFormat getIso8601DateFormat() {
      String ISO8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
      SimpleDateFormat df = new SimpleDateFormat(ISO8601_DATE_FORMAT, Locale.US);
      df.setTimeZone(new SimpleTimeZone(0, "GMT"));
      return df;
    }

    private static DateFormat getAlternativeIso8601DateFormat() {
      String ALTERNATIVE_ISO8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
      SimpleDateFormat df = new SimpleDateFormat(ALTERNATIVE_ISO8601_DATE_FORMAT, Locale.US);
      df.setTimeZone(new SimpleTimeZone(0, "GMT"));
      return df;
    }

}
