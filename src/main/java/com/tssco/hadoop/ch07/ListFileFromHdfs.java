package com.tssco.hadoop.ch07;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ListFileFromHdfs {
    private static final Logger LOGGER = LoggerFactory.getLogger(ListFileFromHdfs.class);

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(args[0]), true);
        if (listFiles != null) {
            while (listFiles.hasNext()) {
                LocatedFileStatus fileStatus = listFiles.next();
                if (fileStatus.isDirectory()) {
                    LOGGER.info("目錄:" + fileStatus.getPath().toString());
                } else {
                    LOGGER.info("文件:" + fileStatus.getPath().toString());
                }
            }
        } else {
            LOGGER.info(args[0] + " => 目錄為空");
        }
        fs.close();
    }

    private static void listFileStatus(FileSystem fs, FileStatus fileStatus) throws IOException {
        if (fileStatus.isFile()) {
            LOGGER.info(" 文件: " + fileStatus.getPath().toString());
        } else if (fileStatus.isDirectory()) {
            Path path = fileStatus.getPath();
            LOGGER.info(" 目錄: " + path.toString());
            FileStatus[] fileStatuses = fs.listStatus(path);
            if (fileStatuses != null && fileStatuses.length > 0) {
                for (int iIdx = 0; iIdx < fileStatuses.length; iIdx++) {
                    listFileStatus(fs, fileStatuses[iIdx]);
                }
            }
        }
    }
}
