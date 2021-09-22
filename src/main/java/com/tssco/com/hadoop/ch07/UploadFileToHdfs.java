import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class UploadFileToHdfs {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadFileToHdfs.class);

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(new Path(args[0]), new Path(args[1]));
        fs.close();
        LOGGER.info("執行完畢");
    }
}
