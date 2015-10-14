package CommonFriends;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class CFReducer extends Reducer<Text, Text, Text, Text> {


  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    Iterator valuesIterator = values.iterator();
    Set<String> friendsList = new HashSet<String>(Arrays.asList(valuesIterator
        .next()
        .toString()
        .split(" ")));

    while (valuesIterator.hasNext()) {
      friendsList.retainAll(new HashSet<String>(Arrays.asList(valuesIterator
          .next()
          .toString()
          .split(" "))));
    }
    String friends = StringUtils.join(friendsList.toArray(), " ");
    context.write(key, new Text(friends));
  }

}
