package com.test.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;

public class GenericOptionsParserExample {

    public static void main(String[] args) throws IOException {
        System.out.println(Arrays.toString(args));

        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String value1 = conf.get("mapreduce.map.memory.mb");
        Boolean value2 = conf.getBoolean("job.test", false);
        System.out.println("value1: " + value1 + " & value2: " + value2);

        String[] remainingArgs = optionsParser.getRemainingArgs(); // 위에서 2개만 get 으로 가져왔으므로 나머지 2개만 출력된다.
        System.out.println(Arrays.toString(remainingArgs));

        // GenericOptionsParser 예제
        // hadoop jar mapreduce-example-1.0.0.jar com.test.hadoop.GenericOptionsParserExample -Dmapreduce.map.memory.mb=4g -Djob.test=true other1 other2
        // 하둡 콘솔 명령어에서 입력한 옵션을 분석하여 하둡 Configuration 을 세팅한다.
    }
}
