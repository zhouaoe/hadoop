package org.apache.hadoop.fs.aliyun.oss;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import java.net.URI;
import static org.apache.hadoop.fs.aliyun.oss.Constants.*;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;

public abstract class AbstractOSSMockTest {
    protected static final String BUCKET = "mock-bucket-oss";
    protected AliyunOSSFileSystem fs;
    protected OSSClient ossClient;
    protected static final String ENDPOINT = "oss-us-west-1.aliyuncs.com";

    protected static final ServiceException NOT_FOUND = new OSSException("Not Found", OSSErrorCode.NO_SUCH_KEY,
            "requestIdtest", "hostIdtest", "headertest", "resourceTypetest", "methodTest");

    @Rule
    public ExpectedException exception = ExpectedException.none();
  
    @Before
    public void setup() throws Exception {
      Configuration conf = createConfiguration();
      conf.set(ENDPOINT_KEY, ENDPOINT, "OSS UT TEST");
      conf.set(ACCESS_KEY_ID,"testaccesskey","OSS UT TEST");
      conf.set(ACCESS_KEY_SECRET,"testSecretkey","OSS UT TEST");

      fs = new AliyunOSSV2FileSystem();
      URI uri = URI.create(FS_OSS + "://" + BUCKET);
      fs.initialize(uri, conf);
      ossClient = fs.getStore().getOSSClient();
    }
 
    public Configuration createConfiguration() {
    Configuration conf = new Configuration();
    conf.setClass(OSS_CLIENT_FACTORY_IMPL, MockOSSClientFactory.class,
        OSSClientFactory.class);

    return conf;
    }

    public OSSClient getOSSClient(){
        return ossClient;
    }
}



