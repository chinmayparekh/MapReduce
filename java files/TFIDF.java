import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.stemmer.PorterStemmer;

import org.apache.hadoop.fs.FileSystem;


public class TFIDF {

	private static HashMap<String,Integer> map = new HashMap<String,Integer>();
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

//		private final static IntWritable zero = new IntWritable(0);
		
		
		PorterStemmer stemmer = new PorterStemmer();
		private boolean caseSensitive = false;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException
		{
		  URI[] cacheFiles = context.getCacheFiles();
		  if (cacheFiles != null && cacheFiles.length > 0)
		  {
			  FileSystem fs = FileSystem.get(context.getConfiguration());
		      Path path = new Path(cacheFiles[0].toString());
		      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
		      System.out.println(reader.readLine());
		      
		      String line;
              while ((line = reader.readLine()) != null)
              {
                  String[] words = line.trim().split("\t");
                  String val = words[0];
                  int DF = Integer.parseInt(words[1]);
                  
                  map.put(val,DF);
              }
              reader.close();
		  }
		}
		
		private Text word = new Text();

		POSModel model = new POSModelLoader().load(new File("/home/chinmay/chin/semester6/NoSQL/final/opennlp-en-ud-ewt-pos-1.0-1.9.3.bin"));
//		POSTaggerME tagger = new POSTaggerME(model);
		

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
			
			String[] tokens = line.split("[^\\w']+");
			for (int i = 0; i < tokens.length; i++) {
			    tokens[i] = stemmer.stem(tokens[i]);
			}
			
			
			MapWritable myMap = new MapWritable();
			
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			
			for(String k:map.keySet())
			{
				myMap.put(new Text(k),new IntWritable(0));
			}
			
			for (String token: tokens) {
				
				if(myMap.containsKey(new Text(token))) {
                    IntWritable temp = (IntWritable)myMap.get(new Text(token));
                    int x = temp.get();
                    x++;
                    temp.set(x);
                    myMap.put(new Text(token),temp);
                } 
				
			}
			context.write(new Text(filename),myMap);
		}
	}

	public static class IntSumReducer extends Reducer<Text, MapWritable,Text, Text> {
//		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			
			MapWritable ret = new MapWritable();
			
			for (MapWritable value : values){

                for(MapWritable.Entry<Writable, Writable> e : value.entrySet()){
                    if (ret.containsKey(e.getKey())) {
                        int i = ((IntWritable)e.getValue()).get();
                        int j = ((IntWritable)ret.get(e.getKey())).get();
                        ret.put(e.getKey(), new IntWritable(i+j));
                    } else {
                        ret.put(e.getKey(), e.getValue());
                    }
                }

            }
			
			for(MapWritable.Entry<Writable, Writable> e : ret.entrySet())
			{
				Text word = new Text(); //a		
				word = (Text) e.getKey();
				int tf = ((IntWritable)e.getValue()).get();//14
				int keyword=((IntWritable) ret.get(e.getKey())).get();
				
				int df = map.get(word.toString());
				
				Text res = new Text();
				String s=String.valueOf(tf*Math.log(50/(df+1)));  
				
				
				res = new Text(word+"\t"+s);
				context.write(key, res);
			}
			
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "tfidf");
		job.setJarByClass(TFIDF.class);
		
		job.addCacheFile(new Path("/home/chinmay/eclipse-workspace/Frequency/src/df_filtered_100.tsv").toUri());
		
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class); // enable to use 'local aggregation'
		job.setReducerClass(IntSumReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}