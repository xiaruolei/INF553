import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Ruolei_Xia_Average {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);
        private static IntWritable page_count = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            if (key.toString().equals("0")) {
                return;
            } else {
                String lines = value.toString();
                String[] list = lines.split(",");

                String event = list[3];
                event = event.toLowerCase().trim();
                event = event.replaceAll("-", "");
                event = event.replaceAll("'", "");
                event = event.replaceAll( "[^0-9a-zA-Z]", " ");
                event = event.replaceAll("\\s+"," ");
                event = event.trim();

                String regex=".*[a-zA-Z]+.*";
                Matcher m= Pattern.compile(regex).matcher(event);

                //System.out.println("*"+event);
                if(m.matches()){
                    String count = list[18];
                    page_count.set(Integer.parseInt(count));

                    context.write(new Text(event), new Text(page_count + "," + one));
                }

            }

        }
    }

    public static class AverageReducer extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (Text val : values) {
                String[] str = val.toString().split(",");
                sum += Integer.parseInt(str[0]);
                count += Integer.parseInt(str[1]);
            }

            double res=(sum*1.0)/count;
            context.write(key, new Text(count + "\t" + String.format("%.3f", res)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Average <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Ruolei_Xia_Average.class);
        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(AverageReducer.class);
        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}




