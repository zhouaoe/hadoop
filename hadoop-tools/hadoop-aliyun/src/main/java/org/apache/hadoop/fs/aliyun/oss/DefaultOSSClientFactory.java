package org.apache.hadoop.fs.aliyun.oss;

import org.apache.hadoop.conf.Configured;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;

public class DefaultOSSClientFactory  extends Configured implements OSSClientFactory {
    @Override
    public OSSClient createOSSClient(String endPoint, CredentialsProvider provider, ClientConfiguration clientConf) {
      OSSClient ossClient = new OSSClient(endPoint,provider,clientConf);
      return ossClient;
    }
  }
