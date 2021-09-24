package com.tssco.hadoop.ch08;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataJoinReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        List<String> departmentNames = new ArrayList<String>();
        String employeeName = "";
        for (Text value : values) {
            String[] vs = value.toString().split(" ");
            if (DataJoinConstants.JOIN_LEFT_FILENAME_TAG.equals(vs[1])) {
                employeeName = vs[0];
            } else if (DataJoinConstants.JOIN_RIGHT_FILENAME_TAG.equals(vs[1])) {
                departmentNames.add(vs[0]);
            }
        }

        for (int iIdx = 0; iIdx < departmentNames.size(); iIdx++) {
            context.write(new Text(employeeName), new Text(departmentNames.get(iIdx)));
        }
    }
}
