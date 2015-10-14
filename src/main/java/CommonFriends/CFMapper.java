package CommonFriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CFMapper extends Mapper<Object, Text, Text, Text> {

  private Text word = new Text();

  @Override
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String[] tokens = value.toString().split("->");
    String person = tokens[0];
    String[] friends = tokens[1].split(" ");

    for (String friend : friends) {
      if (person.compareTo(friend) < 0) {
        context.write(new Text(person + " " + friend), new Text(tokens[1]));
        System.out.println("Mapper: " + person + " " + friend + "**" + new Text(tokens[1]));
      }
      else {
        context.write(new Text(friend + " " + person), new Text(tokens[1]));
        System.out.println("Mapper: " + friend + " " + person + "**" + new Text(tokens[1]));

      }
    }
  }
}

