package WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class WCMapper extends Mapper<Object, Text, Text,IntWritable>{

  private final IntWritable one = new IntWritable(1);
  private Text word = new Text();

  @Override
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer tokens = new StringTokenizer(value.toString());
    while(tokens.hasMoreTokens()){
      word.set(tokens.nextToken().trim());
      context.write(word, one);
    }
  }
}
