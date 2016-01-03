package fr.tassi.projects;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by abdelghani on 31/12/15.
 */
public class WordReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {


    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter r) throws IOException {

        int count = 0 ;
        while(values.hasNext())
        {
            IntWritable i = values.next();
            count += i.get();

        }
        output.collect(key, new IntWritable(count));
    }
}
