package org.apache.hadoop.fs.aliyun.oss;

import java.nio.file.Path;
import java.security.Permissions;

import org.apache.hadoop.fs.aliyun.oss.AliyunOSSV2FileSystem;
import org.apache.hadoop.fs.aliyun.oss.OSS;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class TestAliyunOSSV2FileSystem extends AbstractOSSMockTest {

    @Test
    public void testCreate() throws Exception {
        Path path = new Path("/a.txt");
        FsPermission permission = new Permissions((short) 0777);
        boolean overwrite = true;
        int bufferSize = 512;
        short replication = 1;
        long blockSize = 512;
        Progressable progress = null;
        fs.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
    }
}
