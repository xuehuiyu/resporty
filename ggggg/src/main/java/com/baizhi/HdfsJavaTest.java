package com.baizhi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsJavaTest {
    //文件系统对象
    private FileSystem fileSystem;

    private Configuration configuration;
    @Before
    public  void doBefore() throws IOException, URISyntaxException {
        URI uri = new URI("hdfs://hadoop:9000");
        configuration = new Configuration();
        configuration.set("dfs.replication","1");
        fileSystem = FileSystem.get(uri,configuration);
    }
    /**
     * 测试文件上传
     *
     *
     * org.apache.hadoop.security.AccessControlException: Permission denied: user=HIAPAD, access=WRITE, inode="/baizhi":root:supergroup:drwxr-xr-x
     *
     * 解决方案:
     *     添加虚拟机参数: -DHADOOP_USER_NAME=root
     */
   @Test
    public  void testUpload() throws IOException {
        //从本地拷贝文件
       Path path = new Path("e://bbbb.jpg");
       Path path1 = new Path("/baizhi");
       fileSystem.copyFromLocalFile(path,path1);
   }
    @Test
    public void testUpload1() throws IOException {
        Path path = new Path("/baizhi/aaaa.jpg");
        FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
        InputStream inputStream = new FileInputStream("e://bbbb.jpg");
        IOUtils.copyBytes(inputStream,fsDataOutputStream,configuration);
    }

    @Test
    public void testUpload3() throws IOException {
        Path path = new Path("/baizhi/aaaa.jpg");
        FSDataInputStream open = fileSystem.open(path);
        FileOutputStream fileOutputStream = new FileOutputStream("e://cccc.jpg");
        IOUtils.copyBytes(open,fileOutputStream,configuration);
    }
    @Test
    public void delete() throws IOException {
        Path path = new Path("/baizhi/bbbb.jpg");
        fileSystem.delete(path,true);
    }
    @After
    public void flush() throws IOException {
       fileSystem.close();
    }
}
