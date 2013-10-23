package cn.bcc.test;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.bcc.meta.HadoopConf;

public class SplitTest {



    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        int i=0;
        Text mapKey = new Text();
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {
            
            InetAddress addr = InetAddress.getLocalHost();
            String ip=addr.getHostAddress();
            System.out.println(ip+" : "+value.toString());
            mapKey.set(ip);
            context.write(mapKey, value);
            i++; 
        }

        
        public void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println(i);
        }
    }



    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {


        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
               context.write(key, val);
            }  
        }
        
    
        
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = (new HadoopConf()).getConf();
        String[] otherArgs = {"/user/huyangen/input", "/user/huyangen/output"};
        Job job = new Job(conf, "split tesst");
        job.setNumReduceTasks(5);
        job.setJarByClass(WordCount.class);
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
