package org.apache.hadoop.fs.aliyun.oss;

import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;
import org.apache.hadoop.conf.Configured;
import static org.mockito.Mockito.*;

public class MockOSSClientFactory  extends Configured implements OSSClientFactory {
  @Override
  public OSSClient createOSSClient(String endPoint, CredentialsProvider provider,
      ClientConfiguration clientConf) {
    OSSClient ossMockClient = mock(OSSClient.class);
    return ossMockClient;
  }
}
