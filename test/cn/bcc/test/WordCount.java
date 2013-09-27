package cn.bcc.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import cn.bcc.meta.HadoopConf;

public class WordCount {



    public static class TokenizerMapper extends Mapper<Object, Text, LongWritable, Text> {
        private LongWritable sqlKey = new LongWritable();

        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {
            String[] result = value.toString().split(",");
            long threadId = Long.parseLong(result[0]);
            sqlKey.set(threadId);
            context.write(sqlKey, value);
        }



    }



    public static class IntSumReducer extends Reducer<LongWritable, Text, NullWritable, Text> {


        private MultipleOutputs<NullWritable, Text> mos;

        public void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs(context); 
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }

        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                mos.write(NullWritable.get(), val, key.toString());
                //context.write(NullWritable.get(), val);
            }  
        }
        
    
        
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = (new HadoopConf()).getConf();
        String[] otherArgs = {"/user/huyangen/input", "/user/huyangen/output5"};
        Job job = new Job(conf, "MultipleOutputFormat test");
        job.setJarByClass(WordCount.class);
        JobConf confs = new JobConf(conf, WordCount.class);
        confs.setJar("janus.jar");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
       // job.setOutputFormatClass(SQLMultipleOutputFormat.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
