import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class CheckFileIsExistOnHdfs {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckFileIsExistOnHdfs.class);

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        boolean result = fs.exists(new Path(args[0]));
        String message = result ? "文件存在" : "文件不存在";
        LOGGER.info(args[0] + " => " + message);
        fs.close();
    }
}
