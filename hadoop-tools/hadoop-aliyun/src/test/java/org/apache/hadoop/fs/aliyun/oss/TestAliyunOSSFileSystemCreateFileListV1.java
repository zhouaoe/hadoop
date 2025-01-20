package org.apache.hadoop.fs.aliyun.oss;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.security.Permissions;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.SimpleTimeZone;
import java.util.logging.Logger;

import javax.security.auth.login.Configuration;

import java.util.Date;

import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.aliyun.oss.OSS;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.mockito.Mockito;
import org.mockito.ArgumentMatcher;
import static org.mockito.ArgumentMatchers.argThat;

import com.aliyun.oss.model.HeadObjectRequest;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.GenericRequest;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ListObjectsV2Request;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.ListObjectsV2Result;
import com.aliyun.oss.model.ObjectMetadata;

import static org.apache.hadoop.fs.aliyun.oss.Constants.*;

public class TestAliyunOSSFileSystemCreateFileListV1 extends TestAliyunOSSFileSystemCreateFileListV2 {
  protected int listVersion = LIST_V1;
}
