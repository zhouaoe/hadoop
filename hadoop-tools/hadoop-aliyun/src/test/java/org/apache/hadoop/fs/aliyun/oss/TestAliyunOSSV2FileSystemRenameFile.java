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
import com.aliyun.oss.model.InitiateMultipartUploadRequest;;
import com.aliyun.oss.model.InitiateMultipartUploadResult;

public class TestAliyunOSSV2FileSystemRenameFile extends AbstractOSSMockTest {
  // private static final Logger LOG =
  // LoggerFactory.getLogger(TestAliyunOSSV2FileSystemUT.class);

  protected int listVersion = LIST_V2;

  @Test
  public void testRenameFileNormal() throws Exception {
    System.out.println("====testCreateFileWhenFileDoesNotExist");
    Path sourcePath = new Path("/source.txt");
    String sourcekey = sourcePath.toUri().getPath().substring(1);
    String sourcekeyDir = sourcekey + "/";

    Path targetPath = new Path("/target.txt");
    String targetKey = targetPath.toUri().getPath().substring(1);
    String targetKeyDir = targetKey + "/";

    FsPermission permission = new FsPermission((short) 0777);
    boolean overwrite = true;
    Progressable progress = null;

    //source exist
    ObjectMetadata meta = new ObjectMetadata();
    meta.setLastModified(parseIso8601Date("2018-07-07T08:08:08.008Z"));
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, sourcekey))))
        .thenReturn(meta);
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, sourcekeyDir))))
        .thenThrow(NOT_FOUND);
    setMockList(listVersion, BUCKET, "", false, sourcekeyDir, 100, new ArrayList<>(), new ArrayList<>());
    
    //target does not exist
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, targetKey))))
        .thenThrow(NOT_FOUND);
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, targetKeyDir))))
        .thenThrow(NOT_FOUND);
    setMockList(listVersion, BUCKET, "", false, targetKeyDir, 100, new ArrayList<>(), new ArrayList<>());
    
    //mock ossClient.initiateMultipartUpload
    InitiateMultipartUploadResult initiateMultipartUploadResult = new InitiateMultipartUploadResult();
    initiateMultipartUploadResult.setUploadId("testUploadId");
    Mockito.when(ossClient.initiateMultipartUpload(Mockito.any(InitiateMultipartUploadRequest.class)))
        .thenReturn(initiateMultipartUploadResult);


    

    boolean res = fs.rename(sourcePath, targetPath);
    System.out.println(" testRenameFileNormal " + res);

    // checkGetObjectMetadataCalled(0, requestMatcher(BUCKET, key));
    // checkGetObjectMetadataCalled(1, requestMatcher(BUCKET, keyDir));
    // checkListCalled(listVersion, 1, BUCKET, keyDir);

  }

}
