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
import java.io.InputStream;

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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;

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
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.DeleteObjectsResult;

public class TestAliyunOSSFileSystemDeleteFile extends AbstractOSSMockTest {

  protected int listVersion = LIST_V2;
  @Test
  public void testDeleteNormal() throws Exception {
    System.out.println("====testCreateFileWhenFileDoesNotExist");
    Path path = new Path("/testdir/a.txt");
    String key = path.toUri().getPath().substring(1);
    String keyDir = key + "/";

    Path parentPath = path.getParent();
    String parentKey = parentPath.toUri().getPath().substring(1);
    String parentDir = parentKey+"/";

    System.out.println("testCreate key:" + key);
    FsPermission permission = new FsPermission((short) 0777);
    boolean overwrite = true;
    Progressable progress = null;

    ObjectMetadata meta = new ObjectMetadata();
    meta.setLastModified(parseIso8601Date("2018-07-07T08:08:08.008Z"));

    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, key))))
    .thenAnswer(new Answer<ObjectMetadata>() {
        private int callCount = 0;
        @Override
        public ObjectMetadata answer(InvocationOnMock invocation) throws Throwable {
            callCount++;
            if (callCount <= 1) {
                return meta;
            } 
            throw NOT_FOUND; 
        }
    });
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, keyDir))))
        .thenThrow(NOT_FOUND);
    setMockList(listVersion, BUCKET, "", false, keyDir, 100, new ArrayList<>(), new ArrayList<>());
    setMockList(listVersion, BUCKET, "", false, parentDir, 100, new ArrayList<>(), new ArrayList<>());

    
    boolean res  = fs.delete(path,true);
    assertTrue(res == true);

    Mockito.verify(ossClient, Mockito.times(1))
    .getObjectMetadata(argThat(request -> request.getKey().equals(key)));
    Mockito.verify(ossClient, Mockito.times(0))
    .getObjectMetadata(argThat(request -> request.getKey().equals(keyDir)));
    checkListCalled(listVersion, 0, BUCKET, keyDir);

    Mockito.verify(ossClient, Mockito.times(1)).deleteObject(BUCKET,key);

    Mockito.verify(ossClient, Mockito.times(0))
    .getObjectMetadata(argThat(request -> request.getKey().equals(parentKey)));
    Mockito.verify(ossClient, Mockito.times(1))
    .getObjectMetadata(argThat(request -> request.getKey().equals(parentDir)));
    checkListCalled(listVersion, 1, BUCKET, parentDir);

    //create parent dir
    //Mockito.verify判断ossClient.putObject(bucketName, key, in, dirMeta)调用了一次，参数可以是any
    Mockito.verify(ossClient, Mockito.times(1)).putObject(eq(BUCKET), eq(parentDir), any(InputStream.class), any(ObjectMetadata.class));
  }


  @Test
  public void testDeleteDir() throws Exception {
    System.out.println("====testCreateFileWhenFileDoesNotExist");
    Path path = new Path("/testdir/");
    String key = path.toUri().getPath().substring(1);
    String keyDir = key + "/";

    Path parentPath = path.getParent();
    String parentKey = parentPath.toUri().getPath().substring(1);
    String parentDir = parentKey+"/";

    System.out.println("testCreate key:" + key);
    FsPermission permission = new FsPermission((short) 0777);
    boolean overwrite = true;
    Progressable progress = null;

    ObjectMetadata meta = new ObjectMetadata();
    meta.setLastModified(parseIso8601Date("2018-07-07T08:08:08.008Z"));

    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, key))))
    .thenThrow(NOT_FOUND);
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, keyDir))))
        .thenReturn(meta);
    setMockList(listVersion, BUCKET, "", false, keyDir, 100, Arrays.asList(keyDir), new ArrayList<>());
    setMockList(listVersion, BUCKET, "", false, parentDir, 100, new ArrayList<>(), new ArrayList<>());

    
    DeleteObjectsResult result = new DeleteObjectsResult();
    result.setDeletedObjects(Arrays.asList(keyDir));
    Mockito.when(ossClient.deleteObjects(any())).thenReturn(result);

    boolean res  = fs.delete(path,true);
    assertTrue(res == true);

    Mockito.verify(ossClient, Mockito.times(0)).deleteObject(BUCKET,keyDir);
    //batch delete
    Mockito.verify(ossClient, Mockito.times(1)).deleteObjects(any());

    //create parent dir
    Mockito.verify(ossClient, Mockito.times(0)).putObject(eq(BUCKET), eq(parentDir), any(InputStream.class), any(ObjectMetadata.class));
  }

  @Test
  public void testDeleteDirWithRecursicveFlase() throws Exception {
    System.out.println("====testCreateFileWhenFileDoesNotExist");
    Path path = new Path("/testdir/");
    String key = path.toUri().getPath().substring(1);
    String keyDir = key + "/";

    Path parentPath = path.getParent();
    String parentKey = parentPath.toUri().getPath().substring(1);
    String parentDir = parentKey+"/";

    System.out.println("testCreate key:" + key);
    FsPermission permission = new FsPermission((short) 0777);
    boolean overwrite = true;
    Progressable progress = null;

    ObjectMetadata meta = new ObjectMetadata();
    meta.setLastModified(parseIso8601Date("2018-07-07T08:08:08.008Z"));

    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, key))))
    .thenThrow(NOT_FOUND);
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, keyDir))))
        .thenReturn(meta);
    setMockList(listVersion, BUCKET, "", false, keyDir, 100, Arrays.asList(keyDir), new ArrayList<>());
    setMockList(listVersion, BUCKET, "", false, parentDir, 100, new ArrayList<>(), new ArrayList<>());

    
    DeleteObjectsResult result = new DeleteObjectsResult();
    result.setDeletedObjects(Arrays.asList(keyDir));
    Mockito.when(ossClient.deleteObjects(any())).thenReturn(result);

    boolean res  = fs.delete(path,false);
    assertTrue(res == true);

    Mockito.verify(ossClient, Mockito.times(1)).deleteObject(BUCKET,keyDir);
    Mockito.verify(ossClient, Mockito.times(0)).putObject(eq(BUCKET), eq(parentDir), any(InputStream.class), any(ObjectMetadata.class));
  }
}
