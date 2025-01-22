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
import com.aliyun.oss.model.GenericRequest;
import com.aliyun.oss.model.ObjectMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;

public class TestAliyunOSSFileSystemCreateFileListV2 extends AbstractOSSMockTest {
  // private static final Logger LOG =
  // LoggerFactory.getLogger(TestAliyunOSSV2FileSystemUT.class);

  protected int listVersion = LIST_V2;

  @Test
  public void testCreateFileWhenFileDoesNotExist() throws Exception {
    System.out.println("====testCreateFileWhenFileDoesNotExist");
    Path path = new Path("/a.txt");
    String key = path.toUri().getPath().substring(1);
    String keyDir = key + "/";
    System.out.println("testCreate key:" + key);
    FsPermission permission = new FsPermission((short) 0777);
    boolean overwrite = true;
    Progressable progress = null;

    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, key))))
        .thenThrow(NOT_FOUND);
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, keyDir))))
        .thenThrow(NOT_FOUND);
    setMockList(listVersion, BUCKET, "", false, keyDir, 100, new ArrayList<>(), new ArrayList<>());
    FSDataOutputStream outstream =
        fs.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
    System.out.println("success testCreate");
    assertTrue(outstream != null);
    checkGetObjectMetadataCalled(0, requestMatcher(BUCKET, key));
    checkGetObjectMetadataCalled(1, requestMatcher(BUCKET, keyDir));
    checkListCalled(listVersion, 1, BUCKET, keyDir);
  }

  @Test
  public void testCreateRelativePathFileWhenFileDoesNotExist() throws Exception {
    System.out.println("====testCreateRelativePathFileWhenFileDoesNotExist");
    Path path = new Path("a.txt");
    String key = path.toUri().getPath();
    String keyDir = key + "/";
    System.out.println("testCreate key:" + key);
    FsPermission permission = new FsPermission((short) 0777);
    boolean overwrite = true;
    Progressable progress = null;
    String workingPrefix = "user/hadoop/";
    System.out.println("testCreate workingKey:" + workingPrefix + key);
    System.out.println("testCreate workingDir:" + workingPrefix + keyDir);

    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, workingPrefix + key))))
        .thenThrow(NOT_FOUND);
    Mockito.when(
            ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, workingPrefix + keyDir))))
        .thenThrow(NOT_FOUND);

    setMockList(listVersion, BUCKET, "", false, workingPrefix + keyDir, 100, new ArrayList<>(),
        new ArrayList<>());
    FSDataOutputStream outstream =
        fs.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
    System.out.println("success testCreate");
    assertTrue(outstream != null);
    checkGetObjectMetadataCalled(0, requestMatcher(BUCKET, key));
    checkGetObjectMetadataCalled(0, requestMatcher(BUCKET, keyDir));
    checkListCalled(listVersion, 0, BUCKET, keyDir);
    checkGetObjectMetadataCalled(0, requestMatcher(BUCKET, workingPrefix + key));
    checkGetObjectMetadataCalled(1, requestMatcher(BUCKET, workingPrefix + keyDir));
    checkListCalled(listVersion, 1, BUCKET, workingPrefix + keyDir);
  }

  @Test
  public void testCreateFileWhenFileDoesExist() throws Exception {
    System.out.println("====testCreateFileWhenFileDoesExist");
    Path path = new Path("/a.txt");
    String key = path.toUri().getPath().substring(1);
    String keyDir = key + "/";
    System.out.println("testCreate key:" + key);
    FsPermission permission = new FsPermission((short) 0777);
    boolean overwrite = true;
    Progressable progress = null;

    ObjectMetadata meta = new ObjectMetadata();
    meta.setLastModified(parseIso8601Date("2018-07-07T08:08:08.008Z"));

    // 使用 requestMatcher 创建 ArgumentMatcher
    ArgumentMatcher<GenericRequest> matcher = requestMatcher(BUCKET, key);
    Mockito.when(ossClient.getObjectMetadata(argThat(matcher))).thenReturn(meta);

    setMockList(listVersion, BUCKET, "", false, keyDir, 100, new ArrayList<>(), new ArrayList<>());
    FSDataOutputStream outstream =
        fs.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
    System.out.println("success testCreate");
    assertTrue(outstream != null);

    // 检查 ossClient.getObjectMetadata 的访问次数
    Mockito.verify(ossClient, Mockito.times(0))
        .getObjectMetadata(argThat(requestMatcher(BUCKET, key)));
    Mockito.verify(ossClient, Mockito.times(1))
        .getObjectMetadata(argThat(requestMatcher(BUCKET, keyDir)));
    // overwrite == true ,Head operation should not be called
    checkGetObjectMetadataCalled(0, requestMatcher(BUCKET, key));
    checkGetObjectMetadataCalled(1, requestMatcher(BUCKET, keyDir));
    checkListCalled(listVersion, 1, BUCKET, keyDir);
    checkListCalled(listVersion, 0, BUCKET, key);
  }

  @Test
  public void testCreateFileWhenFileDoesExistForbiddenOverwrite() throws Exception {
    System.out.println("====testCreateFileWhenFileDoesExistForbiddenOverwrite");
    Path path = new Path("/a.txt");
    String key = path.toUri().getPath().substring(1);
    String keyDir = key + "/";
    System.out.println("testCreate key:" + key);
    FsPermission permission = new FsPermission((short) 0777);
    boolean overwrite = false;
    Progressable progress = null;

    ObjectMetadata meta = new ObjectMetadata();
    meta.setLastModified(parseIso8601Date("2017-07-07T08:08:08.008Z"));
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, key))))
        .thenReturn(meta);
    setMockList(listVersion, BUCKET, "", false, keyDir, 100, new ArrayList<>(), new ArrayList<>());
    try {
      FSDataOutputStream outstream =
          fs.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
    } catch (org.apache.hadoop.fs.FileAlreadyExistsException e) {
      System.out.println(e.getMessage());
      assertTrue(e.getMessage().contains("/a.txt already exists"));
      return;
    }

    checkGetObjectMetadataCalled(1, requestMatcher(BUCKET, key));
    checkGetObjectMetadataCalled(0, requestMatcher(BUCKET, keyDir));
    checkListCalled(listVersion, 0, BUCKET, keyDir);
  }

  @Test
  public void testCreateFileWhenDirMarkerExist() throws Exception {
    System.out.println("====testCreateFileWhenDirMarkerExist");
    Path path = new Path("/a.txt");
    String key = path.toUri().getPath().substring(1);
    String keyDir = key + "/";
    System.out.println("testCreate key:" + key);
    FsPermission permission = new FsPermission((short) 0777);
    boolean overwrite = true;
    Progressable progress = null;

    ObjectMetadata meta = new ObjectMetadata();
    meta.setLastModified(parseIso8601Date("2017-07-07T08:08:08.008Z"));
    OSSClient ossMockClient = getOSSClient();
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, key))))
        .thenThrow(NOT_FOUND);
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, keyDir))))
        .thenReturn(meta);
    setMockList(listVersion, BUCKET, "", false, keyDir, 100, new ArrayList<>(), new ArrayList<>());

    try {
      FSDataOutputStream outstream =
          fs.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
    } catch (org.apache.hadoop.fs.FileAlreadyExistsException e) {
      System.out.println(e.getMessage());
      assertTrue(e.getMessage().contains("/a.txt is a directory"));
      return;
    }
    assertTrue(false);
  }

  @Test
  // create file a.txt ,when a.txt/b.txt exist
  public void testCreateFileWhenListIsNotEmpty() throws Exception {
    System.out.println("====testCreateFileWhenListIsNotEmpty");
    Path path = new Path("/a.txt");
    String key = path.toUri().getPath().substring(1);
    String keyDir = key + "/";
    System.out.println("testCreate key:" + key);
    FsPermission permission = new FsPermission((short) 0777);
    boolean overwrite = true;
    Progressable progress = null;

    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, key))))
        .thenThrow(NOT_FOUND);
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, keyDir))))
        .thenThrow(NOT_FOUND);
    setMockList(listVersion, BUCKET, "", false, keyDir, 100, Arrays.asList("b.txt"),
        new ArrayList<>());

    try {
      FSDataOutputStream outstream =
          fs.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
    } catch (org.apache.hadoop.fs.FileAlreadyExistsException e) {
      System.out.println(e.getMessage());
      assertTrue(e.getMessage().contains("/a.txt is a directory"));
      return;
    }
    checkGetObjectMetadataCalled(1, requestMatcher(BUCKET, key));
    checkGetObjectMetadataCalled(1, requestMatcher(BUCKET, keyDir));
    checkListCalled(listVersion, 1, BUCKET, keyDir);
  }
}
