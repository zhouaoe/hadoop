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

import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.ObjectMetadata;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;

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
    String parentDir = parentKey + "/";

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
    setMockList(listVersion, BUCKET, "", false, parentDir, 100, new ArrayList<>(),
        new ArrayList<>());
    boolean res = fs.delete(path, true);
    assertTrue(res);

    Mockito.verify(ossClient, Mockito.times(1))
        .getObjectMetadata(argThat(request -> request.getKey().equals(key)));
    Mockito.verify(ossClient, Mockito.times(0))
        .getObjectMetadata(argThat(request -> request.getKey().equals(keyDir)));
    checkListCalled(listVersion, 0, BUCKET, keyDir);

    Mockito.verify(ossClient, Mockito.times(1)).deleteObject(BUCKET, key);

    Mockito.verify(ossClient, Mockito.times(0))
        .getObjectMetadata(argThat(request -> request.getKey().equals(parentKey)));
    Mockito.verify(ossClient, Mockito.times(1))
        .getObjectMetadata(argThat(request -> request.getKey().equals(parentDir)));
    checkListCalled(listVersion, 1, BUCKET, parentDir);

    //create parent dir
    //Mockito.verify判断ossClient.putObject(bucketName, key, in, dirMeta)调用了一次，参数可以是any
    Mockito.verify(ossClient, Mockito.times(1))
        .putObject(eq(BUCKET), eq(parentDir), any(InputStream.class), any(ObjectMetadata.class));
  }

  @Test
  public void testDeleteDir() throws Exception {
    System.out.println("====testCreateFileWhenFileDoesNotExist");
    Path path = new Path("/testdir/");
    String key = path.toUri().getPath().substring(1);
    String keyDir = key + "/";

    Path parentPath = path.getParent();
    String parentKey = parentPath.toUri().getPath().substring(1);
    String parentDir = parentKey + "/";

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
    setMockList(listVersion, BUCKET, "", false, keyDir, 100, Arrays.asList(keyDir),
        new ArrayList<>());
    setMockList(listVersion, BUCKET, "", false, parentDir, 100, new ArrayList<>(),
        new ArrayList<>());
    DeleteObjectsResult result = new DeleteObjectsResult();
    result.setDeletedObjects(Arrays.asList(keyDir));
    Mockito.when(ossClient.deleteObjects(any())).thenReturn(result);

    boolean res = fs.delete(path, true);
    assertTrue(res);

    Mockito.verify(ossClient, Mockito.times(0)).deleteObject(BUCKET, keyDir);
    //batch delete
    Mockito.verify(ossClient, Mockito.times(1)).deleteObjects(any());

    //create parent dir
    Mockito.verify(ossClient, Mockito.times(0))
        .putObject(eq(BUCKET), eq(parentDir), any(InputStream.class), any(ObjectMetadata.class));
  }

  @Test
  public void testDeleteDirWithRecursicveFlase() throws Exception {
    System.out.println("====testCreateFileWhenFileDoesNotExist");
    Path path = new Path("/testdir/");
    String key = path.toUri().getPath().substring(1);
    String keyDir = key + "/";

    Path parentPath = path.getParent();
    String parentKey = parentPath.toUri().getPath().substring(1);
    String parentDir = parentKey + "/";

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
    setMockList(listVersion, BUCKET, "", false, keyDir, 100, Arrays.asList(keyDir),
        new ArrayList<>());
    setMockList(listVersion, BUCKET, "", false, parentDir, 100, new ArrayList<>(),
        new ArrayList<>());

    DeleteObjectsResult result = new DeleteObjectsResult();
    result.setDeletedObjects(Arrays.asList(keyDir));
    Mockito.when(ossClient.deleteObjects(any())).thenReturn(result);
    boolean res = fs.delete(path, false);
    assertTrue(res);
    Mockito.verify(ossClient, Mockito.times(1)).deleteObject(BUCKET, keyDir);
    Mockito.verify(ossClient, Mockito.times(0))
        .putObject(eq(BUCKET), eq(parentDir), any(InputStream.class), any(ObjectMetadata.class));
  }
}
