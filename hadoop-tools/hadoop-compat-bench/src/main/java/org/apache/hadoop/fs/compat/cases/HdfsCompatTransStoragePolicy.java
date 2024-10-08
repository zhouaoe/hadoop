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
package org.apache.hadoop.fs.compat.cases;

import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.compat.common.*;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@HdfsCompatCaseGroup(name = "TransStoragePolicy")
public class HdfsCompatTransStoragePolicy extends AbstractHdfsCompatCase {
  private static final Logger LOG =
      LoggerFactory.getLogger(HdfsCompatStoragePolicy.class);
  private static final Random RANDOM = new Random();
  private Path dir ;
  private Path file ;
  // private Path sub_dir = new Path("/a/sub_a");

  private String[] policies;
  private String defaultPolicyName = "CLOUD_STD";
  private String policyName= "CLOUD_IA";

  public void init(HdfsCompatEnvironment environment) {
    this.env = environment;
    this.fs = env.getFileSystem();
  }

  @HdfsCompatCaseSetUp
  public void setUp() throws IOException {
    policies =  new  String[]{"CLOUD_STD","CLOUD_IA"};
  }

  @HdfsCompatCasePrepare
  public void prepare() throws IOException {
    dir = new Path(getBasePath(),"/a");
    // file = new Path(getBasePath(),"/a/test_1.log");
    System.out.println("prepare HdfsCompatTransStoragePolicy this.dir="+this.dir);

    BlockStoragePolicySpi policy = fs().getStoragePolicy(this.dir);
    this.defaultPolicyName = (policy == null) ? null : policy.getName();
    System.out.println("prepare defaultPolicyName = "+defaultPolicyName);

    List<String> differentPolicies = new ArrayList<>();
    for (String name : policies) {
      System.out.println("prepare Policy query name= "+name);
      if (!name.equalsIgnoreCase(defaultPolicyName)) {
        differentPolicies.add(name);
      }
    }
    if (differentPolicies.isEmpty()) {
      LOG.warn("There is only one storage policy: " +
          (defaultPolicyName == null ? "null" : defaultPolicyName));
      this.policyName = defaultPolicyName;
    } else {
      this.policyName = differentPolicies.get(
          RANDOM.nextInt(differentPolicies.size()));
    }
    System.out.println("prepare policyName = "+policyName);

  }

  @HdfsCompatCaseCleanup
  public void cleanup()throws Exception{
      System.out.println("cleanup  = "+ dir);
  }


  @HdfsCompatCase
  public void setStoragePolicy() throws IOException {
    System.out.println("setStoragePolicy this.dir="+this.dir);
    System.out.println("setStoragePolicy set defaultPolicyName=" + defaultPolicyName  + " to policyName="+policyName);

    fs().setStoragePolicy(dir, policyName);
    BlockStoragePolicySpi policy = fs().getStoragePolicy(dir);
    Assert.assertEquals("CLOUD_STD", policy.getName());
  }

  @HdfsCompatCase
  public void getStoragePolicy() throws IOException {
    System.out.println("getStoragePolicy this.dir="+this.dir);
    BlockStoragePolicySpi policy = fs().getStoragePolicy(file);
    String initialPolicyName = (policy == null) ? null : policy.getName();
    Assert.assertEquals(policyName, initialPolicyName);
  }

  @HdfsCompatCase
  public void unsetStoragePolicy() throws IOException {
    System.out.println("unsetStoragePolicy sleep 180s this.dir="+this.dir);
    Thread.sleep(180*1000);

    fs().unsetStoragePolicy(dir);
    BlockStoragePolicySpi policy = fs().getStoragePolicy(dir);
    String policyNameAfterUnset = (policy == null) ? null : policy.getName();
    System.out.println("unsetStoragePolicy policyNameAfterUnset="+policyNameAfterUnset);
    System.out.println("unsetStoragePolicy defaultPolicyName=null");
    Assert.assertEquals(null, policyNameAfterUnset);
  }


  @HdfsCompatCase(ifDef = "org.apache.hadoop.fs.FileSystem#satisfyStoragePolicy")
  public void satisfyStoragePolicy() throws IOException {
    sub_dir = new Path(getBasePath(),"/a/sub_a");
    fs().satisfyStoragePolicy(sub_dir);
  }
}