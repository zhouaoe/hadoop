/*
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

package org.apache.hadoop.fs.s3a;

import java.time.Duration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.test.PublicDatasetTestUtils;

/**
 * Constants for S3A Testing.
 */
public interface S3ATestConstants {

  /**
   * Prefix for any cross-filesystem scale test options.
   */
  String SCALE_TEST = "scale.test.";

  /**
   * Prefix for s3a-specific scale tests.
   */
  String S3A_SCALE_TEST = "fs.s3a.scale.test.";

  /**
   * Prefix for FS s3a tests.
   */
  String TEST_FS_S3A = "test.fs.s3a.";

  /**
   * Name of the test filesystem.
   */
  String TEST_FS_S3A_NAME = TEST_FS_S3A + "name";

  /**
   * Run the encryption tests?
   */
  String KEY_ENCRYPTION_TESTS = TEST_FS_S3A + "encryption.enabled";

  /**
   * A property set to true if storage class tests are enabled: {@value }.
   */
  String KEY_STORAGE_CLASS_TESTS_ENABLED = TEST_FS_S3A + "create.storage.class.enabled";

  /**
   * A property set to true if ACL tests are enabled: {@value}.
   */
  String KEY_ACL_TESTS_ENABLED = TEST_FS_S3A + "create.acl.enabled";

  /**
   * A property set to true if V1 tests are enabled: {@value}.
   */
  String KEY_LIST_V1_ENABLED = TEST_FS_S3A + "list.v1.enabled";

  /**
   * A property set to true if content encoding tests are enabled: {@value}.
   */
  String KEY_CONTENT_ENCODING_ENABLED = TEST_FS_S3A + "content.encoding.enabled";

  /**
   * Tell tests that they are being executed in parallel: {@value}.
   */
  String KEY_PARALLEL_TEST_EXECUTION = "test.parallel.execution";

  /**
   * A property set to true in maven if scale tests are enabled: {@value}.
   */
  String KEY_SCALE_TESTS_ENABLED = S3A_SCALE_TEST + "enabled";

  /**
   * The number of operations to perform: {@value}.
   */
  String KEY_OPERATION_COUNT = SCALE_TEST + "operation.count";

  /**
   * The number of directory operations to perform: {@value}.
   */
  String KEY_DIRECTORY_COUNT = SCALE_TEST + "directory.count";

  /**
   * The file count to use in rename/delete tests: {@value}.
   */
  String KEY_FILE_COUNT = SCALE_TEST + "file.count";

  /**
   * The readahead buffer: {@value}.
   */
  String KEY_READ_BUFFER_SIZE = S3A_SCALE_TEST + "read.buffer.size";

  int DEFAULT_READ_BUFFER_SIZE = 16384;

  /**
   * Key for a multi MB test file: {@value}.
   */
  String KEY_CSVTEST_FILE = S3A_SCALE_TEST + "csvfile";

  /**
   * Default path for the multi MB test file: {@value}.
   * @deprecated retrieve via {@link PublicDatasetTestUtils}.
   */
  @Deprecated
  String DEFAULT_CSVTEST_FILE = PublicDatasetTestUtils.DEFAULT_EXTERNAL_FILE;

  /**
   * Example path for unit tests; this is never accessed: {@value}.
   */
  String UNIT_TEST_EXAMPLE_PATH = "s3a://example/data/";

  /**
   * Configuration key for an existing object in a requester pays bucket: {@value}.
   *
   * Accessible via
   * {@link PublicDatasetTestUtils#getRequesterPaysObject(Configuration)}.
   */
  String KEY_REQUESTER_PAYS_FILE = TEST_FS_S3A + "requester.pays.file";

  /**
   * Configuration key for an existing bucket with many objects: {@value}.
   *
   * This is used for tests depending on buckets with a large number of keys.
   */
  String KEY_BUCKET_WITH_MANY_OBJECTS
      = TEST_FS_S3A + "bucket-with-many-objects";

  /**
   * Name of the property to define the timeout for scale tests: {@value}.
   * Measured in seconds.
   */
  String KEY_TEST_TIMEOUT = S3A_SCALE_TEST + "timeout";

  /**
   * Name of the property to define the file size for the huge file
   * tests: {@value}.
   * Measured in KB; a suffix like "M", or "G" will change the unit.
   */
  String KEY_HUGE_FILESIZE = S3A_SCALE_TEST + "huge.filesize";

  /**
   * Name of the property to define the partition size for the huge file
   * tests: {@value}.
   * Measured in KB; a suffix like "M", or "G" will change the unit.
   */
  String KEY_HUGE_PARTITION_SIZE = S3A_SCALE_TEST + "huge.partitionsize";

  /**
   * Size of partitions to upload: {@value}.
   */
  String DEFAULT_HUGE_PARTITION_SIZE = "8M";

  /**
   * The default huge size is small —full 5GB+ scale tests are something
   * to run in long test runs on EC2 VMs. {@value}.
   */
  String DEFAULT_HUGE_FILESIZE = "10M";

  /**
   * The default number of operations to perform: {@value}.
   */
  long DEFAULT_OPERATION_COUNT = 2005;

  /**
   * Default number of directories to create when performing
   * directory performance/scale tests.
   */
  int DEFAULT_DIRECTORY_COUNT = 2;

  /**
   * Default number of files to create when performing
   * delete/rename tests.
   */
  int DEFAULT_FILE_COUNT = 50;

  /**
   * Default policy on scale tests: {@value}.
   */
  boolean DEFAULT_SCALE_TESTS_ENABLED = false;

  /**
   * Fork ID passed down from maven if the test is running in parallel.
   * If a build was also executed with job.id set, this is included in
   * the fork ID.
   */
  String TEST_UNIQUE_FORK_ID = "test.unique.fork.id";
  String TEST_STS_ENABLED = "test.fs.s3a.sts.enabled";

  /**
   * Endpoint for STS testing.
   * @deprecated : Use {@link Constants#ASSUMED_ROLE_STS_ENDPOIN}
   */
  @Deprecated
  String TEST_STS_ENDPOINT = "test.fs.s3a.sts.endpoint";

  /**
   * ACL for S3 Logging; used in some tests: {@value}.
   */
  String LOG_DELIVERY_WRITE = "LogDeliveryWrite";

  /**
   * Timeout in Milliseconds for standard tests: {@value}.
   */
  int S3A_TEST_TIMEOUT = 10 * 60 * 1000;

  /**
   * Timeout in Seconds for Scale Tests: {@value}.
   */
  int SCALE_TEST_TIMEOUT_SECONDS = 30 * 60;

  int SCALE_TEST_TIMEOUT_MILLIS = SCALE_TEST_TIMEOUT_SECONDS * 1000;
  /**
   * Optional custom endpoint for S3A configuration tests.
   * This does <i>not</i> set the endpoint for s3 access elsewhere.
   */
  String CONFIGURATION_TEST_ENDPOINT =
      "test.fs.s3a.endpoint";

  /**
   * Property to set to disable caching.
   */
  String FS_S3A_IMPL_DISABLE_CACHE
      = "fs.s3a.impl.disable.cache";

  /**
   * Duration in seconds for role/session token requests: {@value}.
   */
  int TEST_SESSION_TOKEN_DURATION_SECONDS = 900;

  /**
   * Test session duration as a java 8 Duration.
   */
  Duration TEST_SESSION_TOKEN_DURATION = Duration.ofSeconds(
      TEST_SESSION_TOKEN_DURATION_SECONDS);

  /**
   * Constant bytes being written when Client side encryption KMS is enabled
   * for a test. This bytes written takes into account "EncryptionContext",
   * which contains the algo used for eg:
   * "aws:x-amz-cek-alg":"AES/GCM/NoPadding" , and "KeySpec", which specifies
   * the length of data key. for eg: AES_256 to generate a 256-bit symmetric
   * key.
   *
   * For test using bytesWritten as an assertion this constant value can be
   * used.
   */
  int KMS_KEY_GENERATION_REQUEST_PARAMS_BYTES_WRITTEN = 94;

  /**
   * Build directory property.
   * Value: {@value}.
   */
  String PROJECT_BUILD_DIRECTORY_PROPERTY = "project.build.directory";

  /**
   * AWS ireland region.
   */
  String EU_WEST_1 = "eu-west-1";

  /**
   * System property for root tests being enabled: {@value}.
   */
  String ROOT_TESTS_ENABLED = "fs.s3a.root.tests.enabled";

  /**
   * Default policy on root tests: {@value}.
   */
  boolean DEFAULT_ROOT_TESTS_ENABLED = true;
}
