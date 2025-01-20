package org.apache.hadoop.fs.aliyun.oss;

import java.io.IOException;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;


public interface OSSClientFactory {
    OSSClient createOSSClient(String endPoint, CredentialsProvider provider, ClientConfiguration clientConf)
            throws IOException;

}