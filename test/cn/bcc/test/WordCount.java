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



    public static class TokenizerMapper extends Mapper<Object, Text, NullWritable, LongWritable> {


        NullWritable nullObject=NullWritable.get();
        LongWritable mapOutValue = new LongWritable();
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {
            
            try{
                String line = value.toString();
                if(line == null || line.isEmpty()){
                    context.getCounter("1", "1").increment(1);
                }
                
                int startNum = line.lastIndexOf("/")+1;
                int endNum = line.lastIndexOf(".html");
                
                String querstId= line.substring(startNum, endNum);
                long result=Long.valueOf(querstId.toString());
                mapOutValue.set(result);
               
                context.write(nullObject, mapOutValue);
            }catch(Exception e){
                e.printStackTrace();
            }
            
        }



    }



    public static class IntSumReducer extends Reducer<NullWritable, LongWritable, Text, Text> {


        private MultipleOutputs<LongWritable, NullWritable> mos;
        private static int compileDate = 128;
        NullWritable redOut=NullWritable.get();
        

        public void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs(context); 
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }

        public void reduce(NullWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            for (LongWritable val : values) {
                
                Long fileName=val.get()%compileDate;
                mos.write(val,redOut, fileName.toString());
              
            }  
        }
        
    
        
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = (new HadoopConf()).getConf();
        String[] otherArgs = {"/user/huyangen/input", "/user/huyangen/output"};
        Job job = new Job(conf, "alibaba");
        job.setJarByClass(WordCount.class);
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
       // job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
