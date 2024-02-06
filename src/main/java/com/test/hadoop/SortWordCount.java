package com.test.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SortWordCount extends Configured implements Tool {

    public static class SortMapper extends Mapper<Text, Text, LongWritable, Text> {

        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(Long.parseLong(value.toString())), key); // 키를 단어의 개수로 지정한다.
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t"); // 출력 결과의 구분값을 탭으로 지정한다.

        Job job = Job.getInstance(conf, "SortWordCount");

        job.setJarByClass(SortWordCount.class);
        job.setMapperClass(SortMapper.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);   // 정렬은 map 에서 끝나므로 reduce 는 1개만 동작하게 한다.

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SortWordCount(), args);
        System.exit(exitCode);

        // Sort 예제
        // hadoop jar mapreduce-example-1.0.0.jar com.test.hadoop.SortWordCount /user/hyot/output /user/hyot/sortoutput
        // 텍스트 파일의 WordCount 를 수행한 결과로 단어 개수 오름차순으로 정렬한다.
    }
}
