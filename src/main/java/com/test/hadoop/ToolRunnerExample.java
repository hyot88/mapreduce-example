package com.test.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

public class ToolRunnerExample extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        String value1 = conf.get("mapreduce.map.memory.mb");
        Boolean value2 = conf.getBoolean("job.test", false);
        System.out.println("value1: " + value1 + " & value2: " + value2);

        System.out.println(Arrays.toString(strings));
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int exitCode = ToolRunner.run(new ToolRunnerExample(), args);
        System.exit(exitCode);

        // Tool, ToolRunner 예제
        // hadoop jar mapreduce-example-1.0.0.jar com.test.hadoop.ToolRunnerExample -Dmapreduce.map.memory.mb=4g -Djob.test=true other1 other2
        // GenericOptionsParser 의 콘솔 설정 옵션을 지원하기 위한 인터페이스인 Tool 을 사용했고, 이 인터페이스의 실행을 도와주는 헬퍼 클래스인 ToolRunner 를 사용하였다.
    }
}
