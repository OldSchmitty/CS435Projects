import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordCountPair implements Writable, WritableComparable<WordCountPair>,Comparable<WordCountPair> {
    private Text word;                 // natural key
    private IntWritable count;


    public WordCountPair(Text word, IntWritable count) {
        this.word = word;
        this.count = count;
    }
    public WordCountPair(){
        word = new Text();
        count = new IntWritable();
    }

    public Text getWord() {
        return word;
    }

    public IntWritable getCount() {
        return count;
    }

    public void write (DataOutput output) throws IOException {
        WritableUtils.writeString(output, word.toString());
        WritableUtils.writeString(output, count.toString());
    }

    public void readFields(DataInput input) throws IOException{
        this.word.set(WritableUtils.readString(input));
        this.count.set(WritableUtils.readVInt(input));
    }

    public int compareTo(WordCountPair pair) {
        int compareValue = this.count.compareTo(pair.getCount());
        if (compareValue == 0) {
            compareValue = this.word.compareTo(pair.getWord());
            return compareValue ;    // sort ascending words
        } else {
            return -1 * compareValue;   // sort decending number
        }
    }

}