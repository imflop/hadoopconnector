import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class FileSystemOperations {

    private FileSystemOperations() {}

    private void addFile(Configuration conf, String dest, String source) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        Path path = new Path(dest);

        FSDataOutputStream out = fileSystem.create(path);

        InputStream in = new BufferedInputStream(new FileInputStream(new File(source)));

        int numBytes;
        byte[] b = new byte[1024];
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        in.close();
        out.close();
        fileSystem.close();
    }

    public static void main(String[] args) throws IOException {
        FileSystemOperations client = new FileSystemOperations();
        String hdfsPath = "hdfs://vckp/wso2_esb";

        Configuration conf = new Configuration();
        conf.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
        conf.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
        conf.set("vckp", hdfsPath);

        client.addFile(conf, hdfsPath, "");
    }
}
