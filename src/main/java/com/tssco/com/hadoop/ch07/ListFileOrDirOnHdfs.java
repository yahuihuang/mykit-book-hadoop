import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class ListFileOrDirOnHdfs {
    private static final Logger LOGGER = LoggerFactory.getLogger(ListFileOrDirOnHdfs.class);

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatuses = fs.listStatus(new Path(args[0]));
        if (fileStatuses != null && fileStatuses.length > 0) {
            for (int iIdx = 0; iIdx < fileStatuses.length; iIdx++) {
                FileStatus fileStatus = fileStatuses[iIdx];
                listFileStatus(fs, fileStatus);
            }
        } else {
            LOGGER.info(args[0] + " => 目錄為空");
        }

        fs.close();
    }

    private static void listFileStatus(FileSystem fs, FileStatus fileStatus) throws IOException {
        if (fileStatus.isFile()) {
            LOGGER.info("\t文件: " + fileStatus.getPath().toString());

            LOGGER.info("\n\t取得文件存儲位置");
            BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
            int len = fileBlockLocations.length;
            for (int iIdx = 0; iIdx < len; iIdx++) {
                String[] hosts = fileBlockLocations[iIdx].getHosts();
                LOGGER.info("block_" + iIdx + "_location: " + hosts[0]);
            }

            LOGGER.info("\t----------------------------------------");
        } else if (fileStatus.isDirectory()) {
            Path path = fileStatus.getPath();
            LOGGER.info(" 目錄: " + path.toString());
            FileStatus[] fileStatuses = fs.listStatus(path);
            if (fileStatuses != null && fileStatuses.length > 0) {
                for (int iIdx = 0; iIdx < fileStatuses.length; iIdx++) {
                    listFileStatus(fs, fileStatuses[iIdx]);
                }
            }

            LOGGER.info("###############################################");
        }
    }
}
