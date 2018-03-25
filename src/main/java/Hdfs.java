import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

public class Hdfs {
    public static void main(String[] args) throws Exception {
        //下载文件
        FileSystem fs = FileSystem.get(new URI("hdfs://spark1:9000"),new Configuration());
        InputStream in = fs.open(new Path("/123"));
        FileOutputStream out = new FileOutputStream(new File("D://Test.txt"));
        IOUtils.copyBytes(in,out,4096,true);
        //上传文件
        FileSystem fs1 = FileSystem.get(new URI("hdfs://spark1:9000"),new Configuration(),"root");
        InputStream in1 = new FileInputStream(new File("D://ceshi.txt"));
        FSDataOutputStream out1 = fs1.create(new Path("/ceshi"));
        IOUtils.copyBytes(in1,out1,4096,true);
    }
}
