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
package org.apache.hadoop.fs.compat.suites;

import org.apache.hadoop.fs.compat.common.AbstractHdfsCompatCase;
import org.apache.hadoop.fs.compat.common.HdfsCompatSuite;
import org.apache.hadoop.fs.compat.cases.*;

public class HdfsCompatSuiteForTrans implements HdfsCompatSuite {
  @Override
  public String getSuiteName() {
    return "TRANS";
  }

  @Override
  public Class<? extends AbstractHdfsCompatCase>[] getApiCases() {
    return new Class[]{
        HdfsCompatTransFile.class,
        HdfsCompatTransXAttr.class,
        HdfsCompatTransSymlink.class,
        HdfsCompatTransDirectory.class,
        HdfsCompatTransStoragePolicy.class,
        HdfsCompatTransSnapshot.class,
    };
  }

  @Override
  public String[] getShellCases() {
    return new String[0];
  }
}