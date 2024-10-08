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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.compat.common.*;
import org.junit.Assert;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@HdfsCompatCaseGroup(name = "TransXAttr")
public class HdfsCompatTransXAttr extends AbstractHdfsCompatCase {
  private Path file = new Path("/a/test_1.log");
  public void init(HdfsCompatEnvironment environment) {
    this.env = environment;
    this.fs = env.getFileSystem();
  }


  @HdfsCompatCasePrepare
  public void prepare() throws IOException {
  }

  private void clearXAttr() throws IOException {
    List<String> names = new ArrayList();
    names.addAll(fs().listXAttrs(file));
    System.out.println("names:"+names.toString());
    for (String key : names)
    {
      fs().removeXAttr(file, key);
    }
  }

  @HdfsCompatCaseCleanup
  public void cleanup() throws IOException{
    clearXAttr();
  }

  @HdfsCompatCase
  public void setXAttr() throws IOException {
    final String key = "user.key";
    final byte[] value = "value".getBytes(StandardCharsets.UTF_8);
    fs().setXAttr(file, key, value);
    Map<String, byte[]> attrs = fs().getXAttrs(file);
    Assert.assertArrayEquals(value, attrs.getOrDefault(key, new byte[0]));
  }

  @HdfsCompatCase
  public void getXAttr() throws IOException {
    final String key = "user.key";
    final byte[] value = "value".getBytes(StandardCharsets.UTF_8);
    fs().setXAttr(file, key, value);
    byte[] attr = fs().getXAttr(file, key);
    Assert.assertArrayEquals(value, attr);
  }

  @HdfsCompatCase
  public void getXAttrs() throws IOException {
    fs().setXAttr(file, "user.key1",
        "value1".getBytes(StandardCharsets.UTF_8));
    fs().setXAttr(file, "user.key2",
        "value2".getBytes(StandardCharsets.UTF_8));
    List<String> keys = new ArrayList<>();
    keys.add("user.key1");
    Map<String, byte[]> attrs = fs().getXAttrs(file, keys);
    Assert.assertEquals(1, attrs.size());
    byte[] attr = attrs.getOrDefault("user.key1", new byte[0]);
    Assert.assertArrayEquals("value1".getBytes(StandardCharsets.UTF_8), attr);
  }

  @HdfsCompatCase
  public void listXAttrs() throws IOException {
    fs().setXAttr(file, "user.key2_1",
        "value1".getBytes(StandardCharsets.UTF_8));
    fs().setXAttr(file, "user.key2_2",
        "value2".getBytes(StandardCharsets.UTF_8));
    List<String> names = fs().listXAttrs(file);
    Assert.assertEquals(2, names.size());
    Assert.assertTrue(names.contains("user.key2_1"));
    Assert.assertTrue(names.contains("user.key2_2"));
  }

  @HdfsCompatCase
  public void removeXAttr() throws IOException {
    fs().setXAttr(file, "user.key3_1",
        "value1".getBytes(StandardCharsets.UTF_8));
    fs().setXAttr(file, "user.key3_2",
        "value2".getBytes(StandardCharsets.UTF_8));
    fs().removeXAttr(file, "user.key3_1");
    List<String> names = fs().listXAttrs(file);
    Assert.assertEquals(1, names.size());
    Assert.assertTrue(names.contains("user.key3_2"));
  }
}