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

import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.ObjectMetadata;
import java.util.ArrayList;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;

public class TestAliyunOSSFileSystemRenameFile extends AbstractOSSMockTest {

  protected int listVersion = LIST_V2;

  @Test
  public void testRenameFileNormalFile() throws Exception {
    System.out.println("====testCreateFileWhenFileDoesNotExist");
    Path sourcePath = new Path("/source.txt");
    String sourcekey = sourcePath.toUri().getPath().substring(1);
    String sourcekeyDir = sourcekey + "/";

    Path destPath = new Path("/dest.txt");
    String destKey = destPath.toUri().getPath().substring(1);
    String destKeyDir = destKey + "/";

    FsPermission permission = new FsPermission((short) 0777);
    boolean overwrite = true;
    Progressable progress = null;

    // source exist
    ObjectMetadata meta = new ObjectMetadata();
    meta.setLastModified(parseIso8601Date("2018-07-07T08:08:08.008Z"));
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, sourcekey))))
        .thenAnswer(new Answer<ObjectMetadata>() {
          private int callCount = 0;

          @Override
          public ObjectMetadata answer(InvocationOnMock invocation) throws Throwable {
            callCount++;
            if (callCount <= 2) {
              return meta;
            }
            throw NOT_FOUND;
          }
        });
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, sourcekeyDir))))
        .thenThrow(NOT_FOUND);
    setMockList(listVersion, BUCKET, "", false, sourcekeyDir, 100, new ArrayList<>(),
        new ArrayList<>());

    // dest does not exist
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, destKey))))
        .thenThrow(NOT_FOUND);
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, destKeyDir))))
        .thenThrow(NOT_FOUND);
    setMockList(listVersion, BUCKET, "", false, destKeyDir, 100, new ArrayList<>(),
        new ArrayList<>());

    // mock copy operation
    CopyObjectResult copyRes = new CopyObjectResult();
    Mockito.when(ossClient.copyObject(BUCKET, sourcekey, BUCKET, destKey)).thenReturn(copyRes);

    boolean res = fs.rename(sourcePath, destPath);
    System.out.println(" testRenameFileNormal " + res);

    Mockito.verify(ossClient, Mockito.times(1)).copyObject(BUCKET, sourcekey, BUCKET, destKey);
    // src copy 1 ,delete 1
    Mockito.verify(ossClient, Mockito.times(2))
        .getObjectMetadata(argThat(request -> request.getKey().equals(sourcekey)));
    Mockito.verify(ossClient, Mockito.times(0))
        .getObjectMetadata(argThat(request -> request.getKey().equals(sourcekeyDir)));
    checkListCalled(listVersion, 0, BUCKET, sourcekeyDir);

    // dest copy 1
    Mockito.verify(ossClient, Mockito.times(1))
        .getObjectMetadata(argThat(request -> request.getKey().equals(destKey)));
    Mockito.verify(ossClient, Mockito.times(1))
        .getObjectMetadata(argThat(request -> request.getKey().equals(destKeyDir)));
    checkListCalled(listVersion, 1, BUCKET, destKeyDir);
  }

  @Test
  public void testRenameDirNormal() throws Exception {
    // start state: srcdir/ssrcfile.txt
    // end state: destdir/srcfile.txt srcdir/
    System.out.println("====testCreateFileWhenFileDoesNotExist");
    Path sourcePath = new Path("/srcdir");
    String sourcekey = sourcePath.toUri().getPath().substring(1);
    String sourcekeyDir = sourcekey + "/";

    Path destPath = new Path("/destdir");
    String destKey = destPath.toUri().getPath().substring(1);
    String destKeyDir = destKey + "/";
    Path destParentDir = new Path("/");

    FsPermission permission = new FsPermission((short) 0777);
    boolean overwrite = true;
    Progressable progress = null;

    // source is a dirmarker
    ObjectMetadata meta = new ObjectMetadata();
    meta.setLastModified(parseIso8601Date("2018-07-07T08:08:08.008Z"));
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, sourcekey))))
        .thenThrow(NOT_FOUND);
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, sourcekeyDir))))
        .thenReturn(meta);
    Mockito.when(
            ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, sourcekeyDir + "srcfile.txt"))))
        .thenReturn(meta);
    setMockList(listVersion, BUCKET, "", false, sourcekeyDir, 100, new ArrayList<>(),
        new ArrayList<>());

    // dest does not exist
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, destKey))))
        .thenThrow(NOT_FOUND);
    Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET, destKeyDir))))
        .thenThrow(NOT_FOUND);
    setMockList(listVersion, BUCKET, "", false, destKeyDir, 100, new ArrayList<>(),
        new ArrayList<>());
    // Mockito.when(ossClient.getObjectMetadata(argThat(requestMatcher(BUCKET,
    // destKeyDir))))
    // .thenReturn(meta);

    // mock copy operation
    CopyObjectResult copyRes = new CopyObjectResult();
    Mockito.when(ossClient.copyObject(anyString(), anyString(), anyString(), anyString()))
        .thenReturn(copyRes);
    boolean res = fs.rename(sourcePath, destPath);
    System.out.println(" testRenameFileNormal " + res);
    Mockito.verify(ossClient, Mockito.times(2))
        .getObjectMetadata(argThat(request -> request.getKey().equals(sourcekey)));
  }
}