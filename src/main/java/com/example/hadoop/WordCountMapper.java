package com.example.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Set<String> stopWords = new HashSet<>();
    private Text wordText = new Text();
    private final static LongWritable one = new LongWritable(1);
    private CSVParser csvParser;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        csvParser = new CSVParserBuilder()
                .withSeparator(',')
                .withQuoteChar('"')
                .build();

        String stopWordsPath = context.getConfiguration().get("stopwords.path");
        if (stopWordsPath != null) {
            Path path = new Path(stopWordsPath);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (FSDataInputStream in = fs.open(path);
                 BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                String line;
                while ((line = br.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        try {
            String[] fields = csvParser.parseLine(line);

            if (fields.length >= 2) {
                String headline = fields[1].trim();
                if (!headline.isEmpty()) {
                    String cleaned = headline.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();
                    String[] words = cleaned.split("\\s+");
                    for (String word : words) {
                        if (!word.isEmpty() && !stopWords.contains(word)) {
                            wordText.set(word);
                            context.write(wordText, one);
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error parsing line: " + line);
            e.printStackTrace();
        }
    }
}