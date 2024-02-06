package com.test.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCountWithCounter extends Configured implements Tool {

    static enum Word {
        WITHOUT_SPECIAL_CHARACTER,
        WITH_SPECIAL_CHARACTER
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Pattern pattern = Pattern.compile("[^a-z0-9 ]", Pattern.CASE_INSENSITIVE);

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                String str = itr.nextToken().toLowerCase();
                Matcher matcher = pattern.matcher(str);

                if (matcher.find()) {
                    context.getCounter(Word.WITH_SPECIAL_CHARACTER).increment(1);
                } else {
                    context.getCounter(Word.WITHOUT_SPECIAL_CHARACTER).increment(1);
                }

                word.set(str);
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
            // (hadoop, 3)
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), "wordcount with counter");

        job.setJarByClass(WordCountWithCounter.class);

        job.setMapperClass(WordCountWithCounter.TokenizerMapper.class);
        job.setCombinerClass(WordCountWithCounter.IntSumReducer.class);
        job.setReducerClass(WordCountWithCounter.IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCountWithCounter(), args);
        System.exit(exitCode);

        // Context 의 Counter 예제
        // hadoop jar mapreduce-example-1.0.0.jar com.test.hadoop.WordCountWithCounter /user/hyot/LICENSE.txt /user/hyot/output2
        // 텍스트 파일의 WordCount 를 수행하는 예제에 context 의 counter 를 적용해보는 예제이다.
        // 정규식을 통하여 단어들의 특수문자 포함 여부를 체크한 후, 맵리듀스 결과에 특수문자, 특수문자아닌 단어들의 개수를 보여준다.

        /*
            com.test.hadoop.WordCountWithCounter$Word
                WITHOUT_SPECIAL_CHARACTER=1350
                WITH_SPECIAL_CHARACTER=322
         */
    }
}
