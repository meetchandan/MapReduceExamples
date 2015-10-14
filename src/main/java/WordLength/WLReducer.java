package WordLength;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WLReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

  private IntWritable count = new IntWritable();
  @Override
  public void reduce(IntWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    for(Text val : values){
      sum += 1;
    }
    count.set(sum);
    context.write(key, count);
  }

}
