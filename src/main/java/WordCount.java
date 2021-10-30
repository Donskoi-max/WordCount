import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;


import java.io.*;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;


public class WordCount {
    //    得到全局变量，文件名称list
    static List<String> filenamelist=new ArrayList<String>();
    public static class TokenizerMapper extends
            Mapper<Object, Text, Text, IntWritable>{

        static enum CountersEnum { INPUT_WORDS }
        private final static IntWritable one=new IntWritable(1);
        private Text word =new Text();
        private boolean caseSensitive;
        private final Set<String> patternsToSkip = new HashSet<String>();
        private final Set<String> stopwordsToSkip= new HashSet<String>();
        static List<String> stoplist=new ArrayList<String>();
        static boolean readstop=false;
        static boolean readfilename=false;

        @Override
        public void setup(Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
            if (conf.getBoolean("wordcount.skip.patterns", true)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
//                    System.out.println(patternsFileName);
                    parseSkipFile(patternsFileName);
                }
            }
            if(!readstop){
                //这里的修改始终报错
                InputStream is= Objects.requireNonNull(WordCount.class.getClassLoader().getResource("stop-word-list.txt")).openStream();
                Reader reader = new InputStreamReader(is);
                BufferedReader stopbuffer=new BufferedReader(reader);
//                BufferedReader stopbuffer=new BufferedReader(new FileReader("stop-word-list.txt"));
                String tmp=null;
                while ((tmp=stopbuffer.readLine())!=null){
                    String[] ss =tmp.split(" ");
                    stoplist.addAll(Arrays.asList(ss));
                }
                stopbuffer.close();
                readstop=true;
            }

            if(!readfilename){
                //读取input文件中的文件名，应该也要做修改
//                File inputfile=new File("input");
//                File[] inputfilelist=inputfile.listFiles();
//                assert inputfilelist != null;
//                for (File file : inputfilelist) {
//                    filenamelist.add(file.getName());
//                }
                InputStream is_file= Objects.requireNonNull(WordCount.class.getClassLoader().getResource("filename.txt")).openStream();
                Reader reader = new InputStreamReader(is_file);
                BufferedReader stopbuffer=new BufferedReader(reader);
                String tmp=null;
                while ((tmp=stopbuffer.readLine())!=null){
                    String[] ss =tmp.split(" ");
                    filenamelist.addAll(Arrays.asList(ss));
                }
                stopbuffer.close();
                readfilename=true;
            }
        }
        private void parseSkipFile(String fileName) {
            try {
                BufferedReader fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{
            String line=(caseSensitive)?
                    value.toString():value.toString().toLowerCase();
            for (String pattern : patternsToSkip) {
                line = line.replaceAll(pattern, " ");
            }
            line=line.replaceAll("[0-9][0-9]*"," ");
            StringTokenizer itr=new StringTokenizer(line);
            while (itr.hasMoreTokens()){
                String tmpword= itr.nextToken();
                if(!stoplist.contains(tmpword.toLowerCase())&&(tmpword.length()>=3)){
                    FileSplit inputSplit = (FileSplit) context.getInputSplit();
                    String inputFileName = inputSplit.getPath().getName();
//                System.out.println(inputFileName);
                    word.set(tmpword.toLowerCase());
                    context.write(new Text(word.toString()+"-"+inputFileName),one);
//                    下面是第一题对应的代码
//                    context.write(new Text(word),one);
                    Counter counter = context.getCounter(CountersEnum.class.getName(), CountersEnum.INPUT_WORDS.toString());
                    counter.increment(1);
                }
            }
        }
    }
    public static class IntSumReducer extends
            Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable result=new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            int sum=0;
            for (IntWritable val:values){
                sum+=val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }
    public static class myclass
            implements WritableComparable<myclass>{
        public int x;
        public String y;
        public int getX(){
            return x;
        }
        public String getY(){
            return y;
        }
        public void readFields(DataInput in) throws IOException{
            x = in.readInt();
            y = in.readUTF();
        }
        public void write(DataOutput out) throws IOException {
            out.writeInt(x);
            out.writeUTF(y);
        }
        public int compareTo(myclass p) {
            if (this.x > p.x) {
                return -1;
            } else if (this.x < p.x) {
                return 1;
            } else {
                if (this.getY().compareTo(p.getY()) < 0) {
                    return -1;
                } else if (this.getY().compareTo(p.getY()) > 0) {
                    return 1;
                } else {
                    return 0;
                }

            }
        }
    }
//    public static class Mysorter extends IntWritable.Comparator {
//        public int compare(WritableComparable a, WritableComparable b) {
//            return -super.compare(a, b);
//        }
//
//        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//            return -super.compare(b1, s1, l1, b2, s2, l2);
//        }
//    }
    public static class TokenizerMapperNew
            extends Mapper<Object, Text, myclass, IntWritable> {
        private final IntWritable valueInfo = new IntWritable();
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
//            StringTokenizer itr = new StringTokenizer(value.toString());
            String[] word = value.toString().split("\\s+");
            myclass keyInfo = new myclass();
            keyInfo.x = Integer.parseInt(word[word.length - 1]);
            keyInfo.y = word[word.length - 2];
            valueInfo.set(Integer.parseInt(word[word.length - 1]));
            context.write(keyInfo, valueInfo);
            System.out.println(keyInfo.y);
        }
    }
    public static class WordCountPartitioner extends Partitioner<myclass, IntWritable> {
        @Override
        public int getPartition(myclass keyInfo, IntWritable value, int numPartitions) {

            String raw_word=keyInfo.y.toString();
            String need_word=raw_word.substring(0,raw_word.indexOf("-"));
            String file_name=raw_word.substring(raw_word.indexOf("-")+1);
            int res=0;
            for(int i=0;i< filenamelist.size();i++){
                if(file_name.equals(filenamelist.get(i))){
                    res=i;
                    break;
                }
            }
            return res;
        }
    }
    static int sortcounter=0;
    static boolean title=false;
    static int should_process_num=0;
    static int cur_process_num=0;
    public static class IntSumReducer1
            extends Reducer<myclass,IntWritable,Text,IntWritable> {
        private IntWritable valueInfo = new IntWritable();
        private Text keyInfo = new Text();
        public void reduce(myclass key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            String raw_word=key.y.toString();
            String need_word=raw_word.substring(0,raw_word.indexOf("-"));
            String file_name=raw_word.substring(raw_word.indexOf("-")+1);
//            第一题的代码
//            if (!title){
//                context.write(new Text("总计: "),new IntWritable());
//                title=true;
//            }
//            keyInfo.set(key.y);
//            valueInfo.set(key.x);
//            if (sortcounter<=99) {
//                context.write(new Text(String.valueOf(sortcounter+1)+" "+keyInfo),valueInfo);
//                sortcounter+=1;
//            }
//            得到现在在第几个
            for(int i=0;i<filenamelist.size();i++){
                if(file_name.equals(filenamelist.get(i))){
                    cur_process_num=i;
                    break;
                }
            }
            if(cur_process_num==should_process_num){
                if (!title){
//                    context.write(new Text("总计: "),new IntWritable());
                    context.write(new Text(file_name+":"),new IntWritable());
                    title=true;
                }
                keyInfo.set(key.y);
                valueInfo.set(key.x);
                if (sortcounter<=99) {
                    context.write(new Text(String.valueOf(sortcounter+1)+" "+need_word),valueInfo);
                    sortcounter+=1;
                }
                else {
                    should_process_num+=1;
                    sortcounter=0;
                    title=false;
                }
            }
        }
    }
    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
            System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
            System.exit(2);
        }
        Job job=Job.getInstance(conf,"word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i=0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
        if(job.waitForCompletion(true)){
            Configuration conf1 = new Configuration();
            Job job2 = Job.getInstance(conf1, "sort");
            job2.setJarByClass(WordCount.class);
            job2.setMapperClass(TokenizerMapperNew.class);
            job2.setPartitionerClass(WordCountPartitioner.class);
            job2.setNumReduceTasks(filenamelist.size());
            job2.setReducerClass(IntSumReducer1.class);
            job2.setMapOutputKeyClass(myclass.class);
            job2.setMapOutputValueClass(IntWritable.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job2,new Path(otherArgs.get(1)));
            FileOutputFormat.setOutputPath(job2, new Path("final_output"));
            System.exit(job2.waitForCompletion(true)?0:1);
            System.out.println(filenamelist);
        }
    }
}