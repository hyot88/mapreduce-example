package com.test.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class MapSideJoinDistCache extends Configured implements Tool {

    public static class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        HashMap<String, String> departmentsMap = new HashMap<>();
        Text outKey = new Text();
        Text outValue = new Text();

        // map 함수가 호출되기 전에 실행되는 setup 콜백 함수
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            URI[] uris = context.getCacheFiles();

            for (URI uri : uris) {
                Path path = new Path(uri.getPath());
                loadDepartmentMap(path.getName());
            }
        }

        private void loadDepartmentMap(String fileName) throws IOException {
            String line;

            try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                while ((line = br.readLine()) != null) {
                    String[] split = line.split(",");
                    departmentsMap.put(split[0], split[1]);
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // emp_no, birth_date, first_name, last_name, gender, hire_date, dept_no
            String[] split = value.toString().split(",");

            outKey.set(split[0]);
            String department = departmentsMap.get(split[6]);
            department = department == null ? "Not Found" : department;
            outValue.set(split[2] + "\t" + split[4] + "\t" + department);
            context.write(outKey, outValue);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), "MapSideJoinDistCache");
        job.addCacheFile(new URI("/user/hyot/join/input/departments.txt"));

        job.setJarByClass(MapSideJoinDistCache.class);

        job.setMapperClass(MapSideJoinMapper.class);
        job.setNumReduceTasks(0);   // reduce 는 작업을 안하기에 0 을 세팅한다.

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MapSideJoinDistCache(), args);
        System.exit(exitCode);

        // map 단계에서 데이터를 조인하는 예제
        // hadoop jar mapreduce-example-1.0.0.jar com.test.hadoop.MapSideJoinDistCache /user/hyot/join/input/employees.txt /user/hyot/joinoutput1
        // 입력은 employees.txt, 출력은 employees 와 하둡에 있는 departments 를 조인한 정보
    }
}
