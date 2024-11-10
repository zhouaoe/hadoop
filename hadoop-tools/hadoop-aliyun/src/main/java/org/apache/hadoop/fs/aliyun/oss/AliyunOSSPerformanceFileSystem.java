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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.aliyun.oss.statistics.BlockOutputStreamStatistics;
import org.apache.hadoop.fs.aliyun.oss.statistics.impl.OutputStreamStatistics;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.impl.StatusProbeEnum;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.Progressable;

import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectMetadata;

import org.apache.hadoop.util.SemaphoredDelegatingExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils.intOption;
import static org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils.longOption;
import static org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils.maybeAddTrailingSlash;
import static org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils.objectRepresentsDirectory;
import static org.apache.hadoop.fs.aliyun.oss.Constants.*;
import static org.apache.hadoop.fs.impl.PathCapabilitiesSupport.validatePathCapabilityArgs;

/**
 * Implementation of {@link FileSystem} for <a href="https://oss.aliyun.com">
 * Aliyun OSS</a>, used to access OSS blob system in a filesystem style.
 */
public class AliyunOSSPerformanceFileSystem extends AliyunOSSFileSystem {
  private static final Logger LOG =
      LoggerFactory.getLogger(AliyunOSSPerformanceFileSystem.class);
  private URI uri;
  private String bucket;
  private String username;
  private Path workingDir;
  private OSSDataBlocks.BlockFactory blockFactory;
  private BlockOutputStreamStatistics blockOutputStreamStatistics;
  private int uploadPartSize;
  private int blockOutputActiveBlocks;
  private AliyunOSSFileSystemStore store;
  private int maxKeys;
  private int maxReadAheadPartNumber;
  private int maxConcurrentCopyTasksPerDir;
  private ExecutorService boundedThreadPool;
  private ExecutorService boundedCopyThreadPool;


  private FileStatus innerOssGetFileStatus(final Path path,
      final Set<AliyunOSSStatusProbeEnum> probes,
      final boolean needEmptyDirectoryFlag) throws IOException {
    LOG.debug("OssGetFileStatus {}", path);
    final Path qualifiedPath = path.makeQualified(uri, workingDir);
    String key = pathToKey(qualifiedPath);
    LOG.debug("Getting path status for {}  ({});", qualifiedPath, key);

    //Can only determine if it is empty through a list operation
    Preconditions.checkArgument(needEmptyDirectoryFlag && !probes.contains(List));
    
    // Root always exists
    if (key.length() == 0) {
      return new OSSFileStatus(0, true, 1, 0, 0, qualifiedPath, username);
    }

    if (!key.endsWith("/") && probes.contains(AliyunOSSStatusProbeEnum.Head)) {
      try {
        // look for the simple file
        ObjectMetadata meta = store.getObjectMetadata(key);

        // meta will not be null.if data is not exist,therer will throw exception
        LOG.debug("Found exact file: normal file {}", key);
        return new OSSFileStatus(meta.getContentLength(), false, 1, getDefaultBlockSize(path),
            meta.getLastModified().getTime(), qualifiedPath, username);
      } catch (OSSException e) {
        // if file is not exist，continue to do others actions
        if (e.getErrorCode() != OSSErrorCode.NO_SUCH_KEY) {
          throw new IOException("getFileStatus " + path, e);
        }
      }
    }

    // if a DirMarker can be found through a Head operation, it can reduce one list
    // operation. Head operations are relatively lighter than list operations, so
    // doing one more Head operation can be beneficial.
    String dirKey = maybeAddTrailingSlash(key);
    if (needEmptyDirectoryFlag == false &&!dirKey.isEmpty() && probes.contains(AliyunOSSStatusProbeEnum.DirMarker)) {
      try {
        // look for the DirMarker
        ObjectMetadata meta = store.getObjectMetadata(dirKey);
        LOG.debug("Found exact file: DirMarker file {}", dirKey);
        return new OSSFileStatus(meta.getContentLength(), true, 1, getDefaultBlockSize(path),
            meta.getLastModified().getTime(), qualifiedPath, username);
      } catch (OSSException e) {
        // if file is not exist，continue to do others actions
        if (e.getErrorCode() != OSSErrorCode.NO_SUCH_KEY) {
          throw new IOException("getFileStatus DirMarker " + path, e);
        }
      }
    }

    // execute the list
    if (probes.contains(AliyunOSSStatusProbeEnum.List)) {
      // list size is dir marker + at least one entry
      final int listSize = 2;
      OSSListRequest listRequest = store.createListObjectsRequest(dirKey,
          listSize, null, null, false);
      OSSListResult listResult = store.listObjects(listRequest);
      if (CollectionUtils.isNotEmpty(listResult.getObjectSummaries()) ||
          CollectionUtils.isNotEmpty(listResult.getCommonPrefixes())) {
          if(listResult.representsEmptyDirectory(dirKey))
          {
            return new OSSFileStatus(AliyunOSSDirEmptyStatus::EMPTY,0, 1, 0, 0, qualifiedPath, username);
          }
        return new OSSFileStatus(0, true, 1, 0, 0, qualifiedPath, username);
      }
    }

    LOG.debug("Not Found: {}", path);
    throw new FileNotFoundException("No such file or directory: " + path);
  }


  @Override
  public FSDataOutputStream create(Path path, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    String key = pathToKey(path);
    FileStatus status = null;

    try {
      // get the status or throw a FNFE
      // status = getFileStatus(path);
      status = innerOssGetFileStatus(path,
          overwrite ? AliyunOSSStatusProbeEnum.DIRECTORIES : AliyunOSSStatusProbeEnum.ALL ,false);

      //Directory can not be overwrited
      if (status.isDirectory()) {
        throw new FileAlreadyExistsException(path + " is a directory");
      }
      if (!overwrite) {
        // path references a file and overwrite is disabled
        throw new FileAlreadyExistsException(path + " already exists");
      }
      LOG.debug("Overwriting file {}", path);
    } catch (FileNotFoundException e) {
      // this means the file is not found
    }

    return new FSDataOutputStream(
        new AliyunOSSBlockOutputStream(getConf(),
            store,
            key,
            uploadPartSize,
            blockFactory,
            blockOutputStreamStatistics,
            new SemaphoredDelegatingExecutor(boundedThreadPool,
                blockOutputActiveBlocks, true)),
        statistics);
  }


  // private boolean isReduceQps(){
  //   return getConf().getBoolean(FS_OSS_FS_REDUCE_QPS, FS_OSS_FS_REDUCE_QPS_DEFALUT_VALUE);
  // }


  @Override
  public boolean rename(Path srcPath, Path dstPath) throws IOException {
    if (srcPath.isRoot()) {
      // Cannot rename root of file system
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot rename the root of a filesystem");
      }
      return false;
    }
    Path parent = dstPath.getParent();
    while (parent != null && !srcPath.equals(parent)) {
      parent = parent.getParent();
    }
    if (parent != null) {
      return false;
    }
    FileStatus srcStatus = innerOssGetFileStatus(srcPath, AliyunOSSStatusProbeEnum.ALL);

    FileStatus dstStatus;
    try {
      dstStatus = innerOssGetFileStatus(dstPath, AliyunOSSStatusProbeEnum.ALL);
    } catch (FileNotFoundException fnde) {
      dstStatus = null;
    }
    if (dstStatus == null) {
      // If dst doesn't exist, check whether dst dir exists or not
      //this raise a FileNotFoundException if dst dir does not exist
      FileStatus dstParentStatus = innerOssGetFileStatus(dstPath.getParent(), AliyunOSSStatusProbeEnum.ALL, false);
      if (!dstParentStatus.isDirectory()) {
        throw new IOException(String.format(
            "Failed to rename %s to %s, %s is a file", srcPath, dstPath,
            dstPath.getParent()));
      }
    } else {
      if (srcStatus.getPath().equals(dstStatus.getPath())) {
        return !srcStatus.isDirectory();
      } else if (dstStatus.isDirectory()) {
        // If dst is a directory
        dstDirWithsrcPath = new Path(dstPath, srcPath.getName());

        OSSFileStatus dstDirWithsrcPathStatus;
        try {
          dstDirWithsrcPathStatus = innerOssGetFileStatus(dstPath, AliyunOSSStatusProbeEnum.LIST_ONLY, true);
        } catch (FileNotFoundException fnde) {
          dstDirWithsrcPathStatus = null;
        }

        if (dstDirWithsrcPathStatus != null
            && dstDirWithsrcPathStatus.getEmptyStatus() == AliyunOSSDirEmptyStatus::EMPTY) {
          // If dst exists and not a directory / not empty
          throw new FileAlreadyExistsException(String.format(
              "Failed to rename %s to %s, file already exists or not empty!",
              srcPath, dstPath));
        }
      } else {
        // If dst is not a directory
        throw new FileAlreadyExistsException(String.format(
            "Failed to rename %s to %s, file already exists!", srcPath,
            dstPath));
      }
    }

    boolean succeed;
    if (srcStatus.isDirectory()) {
      succeed = copyDirectory(srcPath, dstPath);
    } else {
      succeed = copyFile(srcPath, srcStatus.getLen(), dstPath);
    }

    return srcPath.equals(dstPath) || (succeed && delete(srcPath, true));
  }


  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    try {
      OssFileStatus ossFileStatus= innerOssGetFileStatus(dstPath, AliyunOSSStatusProbeEnum.ALL, true)
      return innerDelete(ossFileStatus, recursive);
    } catch (FileNotFoundException e) {
      LOG.debug("Couldn't delete {} - does not exist", path);
      return false;
    }
  }

  private boolean ossExists(final Path path, final Set<AliyunOSSStatusProbeEnum> probes)
      throws IOException {
    String key = pathToKey(path);
    try {
      innerOssGetFileStatus(path, key, probes, false);
      return true;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * Delete an object. See {@link #delete(Path, boolean)}.
   *
   * @param status ossFileStatus object
   * @param recursive if path is a directory and set to
   * true, the directory is deleted else throws an exception. In
   * case of a file the recursive can be set to either true or false.
   * @return  true if delete is successful else false.
   * @throws IOException due to inability to delete a directory or file.
   */
  private boolean innerDelete(OssFileStatus ossFileStatus, boolean recursive)
      throws IOException {
    Path f = ossFileStatus.getPath();
    String p = f.toUri().getPath();
    FileStatus[] statuses;
    // indicating root directory "/".
    if (p.equals("/")) {
      LOG.error("OSS: Cannot delete the root directory."
                + " Path: {}. Recursive: {}",
                ossFileStatus.getPath(), recursive);
      return false;
    }

    String key = pathToKey(f);
    if (ossFileStatus.isDirectory()) {
      if (!recursive) {
        // Check whether it is an empty directory or not
        if (ossFileStatus.getEmptyStatus() != AliyunOSSDirEmptyStatus::NOT_EMPTY) {
          throw new IOException("Cannot remove directory " + f +
              ": It is not empty!");
        } else {
          // Delete empty directory without '-r'
          key = AliyunOSSUtils.maybeAddTrailingSlash(key);
          store.deleteObject(key);
        }
      } else {
        store.deleteDirs(key);
      }
    } else {
      store.deleteObject(key);
    }

    createFakeDirectoryIfNecessary(f);
    return true;
  }

  protected void createFakeDirectoryIfNecessary(Path f) throws IOException {
    String key = pathToKey(f);
    LOG.debug("createFakeDirectoryIfNecessary  new fake directory at f={} key={}", f, key);

    if (StringUtils.isNotEmpty(key) && !OssExists(f, AliyunOSSStatusProbeEnum::DIRECTORIES)) {
      LOG.debug("Creating new fake directory at {}", f);
      mkdir(pathToKey(f.getParent()));
    }
  }

  @Override
  @SuppressWarnings("deprecation")
  public boolean isDirectory(Path f) throws IOException {
    final Path path = qualify(f);
    LOG.debug("isDirectory: qualified path={} path={}", path, f);

    try {
      OSSFileStatus fileStatus = innerOssGetFileStatus(path, AliyunOSSStatusProbeEnum::DIRECTORIES, false);
      return fileStatus.isDirectory();
    } catch (FileNotFoundException e) {
      LOG.debug("isDirectory: qualified path={} path={}. not found or it is a file", path, f);
      return false; // f does not exist
    }
  }

  @Override
  @SuppressWarnings("deprecation")
  public boolean isFile(Path f) throws IOException {
    final Path path = qualify(f);
    LOG.debug("isFile: qualified path={} path={}", path, f);
    try {
      return innerOssGetFileStatus(path, AliyunOSSStatusProbeEnum.FILE).isFile();
    } catch (FileNotFoundException e) {
      // not found or it is a dir.
      LOG.debug("isFile: qualified path={} path={}. not found or it is a dir", path, f);
      return false;
    }
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    final Path path = qualify(f);
    //if f does not exist, there will throw a FileNotFoundException
    //if f is a file, fileStatus.isDirectory will be false
    //if f is a dir, throw a FileNotFoundException
    final OSSFileStatus fileStatus = innerOssGetFileStatus(path, AliyunOSSStatusProbeEnum.FILE);
    if (fileStatus.isDirectory()) {
      throw new FileNotFoundException("Can't open " + path +
          " because it is a directory");
    }

    return new FSDataInputStream(new AliyunOSSInputStream(getConf(),
        new SemaphoredDelegatingExecutor(
            boundedThreadPool, maxReadAheadPartNumber, true),
        maxReadAheadPartNumber, store, pathToKey(path), fileStatus.getLen(),
        statistics));
  }
}
