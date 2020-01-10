import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.io.IOException;
import java.util.HashMap;


public class Job5 {


    /*public static class Job5SentenceMapper extends Mapper< Object, Text, Text, Text > {
        private HashMap<String, Double> TFIDFmap = new HashMap<String, Double>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
                URI[] paths = context.getCacheFiles();
                String path = paths[0].toString();

                try {
                    BufferedReader br = new BufferedReader(new FileReader(path));
                    String line = null;
                    while ((line = br.readLine()) != null) {
                        String[] strArray = line.split("\\s+");
                        String docID = strArray[0];
                        String word = strArray[1];
                        String TFIDF = strArray[2];
                        TFIDFmap.put(docID + "\t" + word, Double.parseDouble(TFIDF));

                    }
                } catch (Exception e) {
                    System.out.println(e);
                }
        }


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            TreeMap<StringDoublePair, Integer> sentenceMap = new TreeMap<StringDoublePair, Integer>();
            TreeMap<StringDoublePair, Integer> wordMap = new TreeMap<StringDoublePair, Integer>();
            if (!value.toString().isEmpty()) {
                String[] strArray = value.toString().split("<====>");
                String docID = strArray[1];
                int lineCount = 0;
                strArray = strArray[2].split("\\.\\s+");
                for (String sentence : strArray){
                    lineCount++;
                    StringTokenizer token = new StringTokenizer(sentence);
                    while (token.hasMoreTokens()) {
                        String word = token.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                        if (word.compareTo("")  != 0){
                            double TFIDF = TFIDFmap.get(docID + "\t" + word);
                            StringDoublePair pair = new StringDoublePair(word, TFIDF);
                            wordMap.put(pair, 1);
                            while (wordMap.size() > 5) {
                                StringDoublePair delKey = wordMap.lastKey();
                                wordMap.remove(delKey);
                            }
                        }
                    }
                    double sum = 0;
                    for (StringDoublePair pair : wordMap.keySet()){
                        sum += pair.num;
                    }
                    StringDoublePair sentencePair = new StringDoublePair(sentence, sum);
                    sentenceMap.put(sentencePair, lineCount);
                    while (sentenceMap.size() > 3){
                        sentenceMap.remove(sentenceMap.lastKey());
                    }
                }
                Text outKey = new Text(docID);
                Text outVal= new Text();
                String outStr = "";
                int min = -1;
                while(sentenceMap.size() >0) {
                    StringDoublePair minPair = new StringDoublePair("default", 0);
                    for (StringDoublePair pair : sentenceMap.keySet()) {
                        if (min == -1 || (min !=  -1 && sentenceMap.get(pair) < min)){
                            minPair = pair;
                        }
                    }
                    outStr += minPair.str + ".";
                    min = -1;
                    sentenceMap.remove(minPair);
                }
                outVal.set(outStr);
                context.write(outKey, outVal);
            }
        }
    }*/



    public static class Job5SentenceMapper extends Mapper< Object, Text, Text, Text > {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!value.toString().isEmpty()) {
                String[] strArray = value.toString().split("<====>");
                String docID = strArray[1];
                Text outKey = new Text(docID);
                context.write(outKey, value);
            }

        }

    }

    public static class Job5TFIDFMapper extends Mapper< Object, Text, Text, Text > {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!value.toString().isEmpty()) {
                String[] strArray = value.toString().split("\\s+");
                if (strArray.length  == 3) {
                    String docID = strArray[0];
                    String word = strArray[1];
                    String TFIDF = strArray[2];
                    Text outVal = new Text(word + "\t" + TFIDF);
                    Text outKey = new Text(docID);
                    context.write(outKey, outVal);
                }
            }

        }

    }


    public static class Job5Reducer extends Reducer<Text, Text, Text, Text> {

        private class StringDoublePair implements Comparable<StringDoublePair> {
            public double num;
            public String str;

            public StringDoublePair(String str, double num) {
                this.str = str;
                this.num = num;
            }

            public int compareTo(StringDoublePair pair) {
                if (this.num > pair.num) {
                    return -1;
                }
                else if (this.num < pair.num){
                    return 1;
                }
                else{
                    return this.str.compareTo(pair.str);
                }
            }

        }
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            HashMap<String, Double> TFIDFmap = new HashMap<String, Double>();
            ArrayList<String> list = new ArrayList<String>();
            TreeMap<StringDoublePair, Integer> sentenceMap;
            TreeMap<StringDoublePair, Integer> wordMap;
            String docID = key.toString();

            int count = 0;
            for(Text val : values){
                if (val.toString().contains("<====>")) {
                    list.add(val.toString());
                }
                else{
                    String[] strArray = val.toString().split("\\s+");
                    String word = strArray[0];
                    double TFIDF = Double.parseDouble(strArray[1]);
                    TFIDFmap.put(word, TFIDF);
                }
            }
            for (String value : list){
                sentenceMap = new TreeMap<StringDoublePair, Integer>();
                if (!value.isEmpty()) {
                    String[] strArray = value.toString().split("<====>");
                    int lineCount = 0;
                    strArray = strArray[2].split("\\.\\s+");
                    for (String sentence : strArray) {
                        wordMap = new TreeMap<StringDoublePair, Integer>();
                        StringTokenizer token = new StringTokenizer(sentence);
                        while (token.hasMoreTokens()) {
                            String word = token.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                            if (TFIDFmap.containsKey(word)) {
                                double TFIDF = TFIDFmap.get(word);
                                StringDoublePair pair = new StringDoublePair(word, TFIDF);
                                wordMap.put(pair, 1);
                                while (wordMap.size() > 5) {
                                    wordMap.remove(wordMap.lastKey());
                                }
                            }
                        }
                        double sum = 0;
                        for (StringDoublePair pair : wordMap.keySet()) {
                            sum += pair.num;
                        }
                        StringDoublePair sentencePair = new StringDoublePair(sentence, sum);
                        sentenceMap.put(sentencePair, lineCount);
                        while (sentenceMap.size() > 3) {
                            sentenceMap.remove(sentenceMap.lastKey());
                        }
                    }
                    Text outKey = new Text(docID);
                    Text outVal = new Text();
                    String outStr = "";
                    int min = -1;
                    ArrayList<StringDoublePair> pairList = new ArrayList<StringDoublePair>();
                    StringDoublePair minPair = new StringDoublePair ("default",0);
                    while (sentenceMap.size() > 0) {
                        StringDoublePair copy = new StringDoublePair(sentenceMap.firstKey().str, sentenceMap.get(sentenceMap.firstKey()));
                        pairList.add(copy);
                        sentenceMap.remove(sentenceMap.firstKey());
                    }
                    while (pairList.size() > 0) {
                        int index = 0;
                        min = -1;
                        for (StringDoublePair pair : pairList) {
                            if (min == -1) {
                                minPair = pair;
                                min = index;
                            } else if (pair.num < min) {
                                minPair = pair;
                                min = index;
                            }
                            index++;
                        }
                        outStr+=minPair.str+".";
                        pairList.remove(min);
                    }
                    outVal.set(outStr);
                    context.write(outKey, outVal);
                }
            }
        }
    }
}

