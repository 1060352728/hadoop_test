package com.likui.test.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;;

/**
 * @Auther: likui
 * @Date: 2019/4/23 21:34
 * @Description:
 */
public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://172.17.0.2:8020";
    Configuration configuration = null;
    FileSystem fileSystem = null;

    @Before
    public void setUp () throws Exception {
        System.out.println("----------------------setUp-------------------------");
        configuration = new Configuration();
        configuration.set("dfs.replication","1");
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,"root");
    }

    @After
    public void tearDown(){
        configuration = null;
        fileSystem = null;
        System.out.println("----------------------tearDown-------------------------");
    }

    /**
     *
     * 功能描述: 创建文件夹
     *
     * @param:
     * @return:
     * @auther: likui
     * @date: 2019/4/29 19:47
     */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/testfile"));
    }

    /**
     *
     * 功能描述: 打开文件
     *
     * @param:
     * @return:
     * @auther: likui
     * @date: 2019/4/29 19:47
     */
    @Test
    public void open() throws Exception {
        FSDataInputStream fis = fileSystem.open(new Path("/wc.out"));
        IOUtils.copyBytes(fis,System.out,1024);
    }

    /**
     *
     * 功能描述: 
     *
     * @param: 创建文件并写入数据
     * @return: 
     * @auther: likui
     * @date: 2019/4/29 19:49
     */
    @Test
    public void creat() throws Exception {
        FSDataOutputStream out = fileSystem.create(new Path("/b.txt"));
        out.writeUTF("hello111111");
        out.flush();
        out.close();
    }

    /**
     *
     * 功能描述: 修改文件名
     *
     * @param:
     * @return:
     * @auther: likui
     * @date: 2019/4/29 19:49
     */
    @Test
    public void rename() throws Exception {
        fileSystem.rename(new Path("/b.txt"),new Path("/c.txt"));
    }

    /**
     *
     * 功能描述:从本地copy到hdfs
     *
     * @param:
     * @return:
     * @auther: likui
     * @date: 2019/4/29 19:50
     */
    @Test
    public void copyFromLocalFile() throws Exception {
        fileSystem.copyFromLocalFile(new Path("C:\\Users\\likui\\Desktop\\testfile.txt"),new Path("/test/c.txt"));
    }

    /**
     *
     * 功能描述: 大文件带进度条从本地copy到hdfs
     *
     * @param:
     * @return:
     * @auther: likui
     * @date: 2019/4/29 19:50
     */
    @Test
    public void copyFromLocalBigFile() throws Exception {
        InputStream in = new BufferedInputStream(new FileInputStream(new File("C:\\Users\\likui\\Downloads\\hadoop-2.6.0-cdh5.15.1.tar.gz")));
        FSDataOutputStream out = fileSystem.create(new Path("/hadoop.tar.gz"), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, out, 4096);
    }

    /**
     *
     * 功能描述: 从hdfs copy至本地
     *
     * @param:
     * @return:
     * @auther: likui
     * @date: 2019/4/29 20:00
     */
    @Test
    public void copyToLocalFile() throws Exception {
        fileSystem.copyToLocalFile(false, new Path("/b.txt"), new Path("C:\\Users\\likui\\Downloads\\b.txt"), true);
    }

    /**
     *
     * 功能描述: 查看所有文件
     *
     * @param:
     * @return:
     * @auther: likui
     * @date: 2019/4/29 20:19
     */
    @Test
    public void fileList() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            String directory = fileStatus.isDirectory() == true ? "文件夹":"文件";//是否为文件
            String permission = fileStatus.getPermission().toString();//权限
            short replication = fileStatus.getReplication();//副本数
            long length = fileStatus.getLen();//大小
            String path = fileStatus.getPath().toString();//文件全路径
            System.out.println(directory + "\t" + permission + "\t" + replication  + "\t" + length + "\t" + path);
        }
    }

    /**
     *
     * 功能描述: 递归跌点所有的文件
     *
     * @param:
     * @return:
     * @auther: likui
     * @date: 2019/4/29 20:48
     */
    @Test
    public void fileListRecursive() throws Exception {
        RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(new Path("/"), true);
        while (remoteIterator.hasNext()) {
            LocatedFileStatus file = remoteIterator.next();
            String directory = file.isDirectory() == true ? "文件夹":"文件";//是否为文件
            String permission = file.getPermission().toString();//权限
            short replication = file.getReplication();//副本数
            long length = file.getLen();//大小
            String path = file.getPath().toString();//文件全路径
            System.out.println(directory + "\t" + permission + "\t" + replication  + "\t" + length + "\t" + path);
        }
    }

    /**
     *
     * 功能描述: 得到文件块的大小和节点路径
     *
     * @param:
     * @return:
     * @auther: likui
     * @date: 2019/4/29 20:54
     */
    @Test
    public void getFileBlockLocations() throws Exception {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hadoop.tar.gz"));
        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus,0,fileStatus.getLen());
        for (BlockLocation blockLocation : blockLocations) {
            String[] names = blockLocation.getNames();
            for (String name : names) {
                System.out.println(name + "\t" + blockLocation.getOffset() + "\t" + blockLocation.getLength() + "\t" + blockLocation.getHosts());
            }
        }
    }

    /**
     *
     * 功能描述:  删除操作
     *
     * @param:
     * @return:
     * @auther: likui
     * @date: 2019/4/29 21:08
     */
    @Test
    public void delete()  throws Exception {
        boolean flag = fileSystem.delete(new Path("/hadoop.tar.gz"), false);
        System.out.println(flag);
    }
}
