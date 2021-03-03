import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;


/* For constructing the solution */

public class AirQualityMonitor2 {

    public static final String LEVEL_0 = "-1 ";
    public static final String LEVEL_1 = "0 ";
    public static final String LEVEL_2 = "1 ";
    public static final String LEVEL_3 = "2 ";
    public static final String LEVEL_4 = "3 ";
    public static final String LEVEL_5 = "4 ";
    public static final String LEVEL_6 = "5 ";

    public static final String DIS_0 = "No discrepancy: ";
    public static final String DIS_1 = "Discrepancies of 1 level: ";
    public static final String DIS_2 = "Discrepancies of >=2 levels: ";
    public static final String DIS_3 = "Either one location has invalid reading: ";
    public static final String DIS_4 = "Total entries: ";

    public static class AirQualityMapper1
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            /* get the file name */
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
//            context.write(new Text(filename), new Text(""));

            StringTokenizer itr = new StringTokenizer(value.toString()); // each time the value is a different line
            String ym = itr.nextToken(); // get the first column - year month indicator
            int yearMonth = Integer.parseInt(ym);

            String output = new String();
            if (yearMonth == 0) {
                return; // skip the first row 0
            } else {
                while (itr.hasMoreTokens()) {
                    String sVal = itr.nextToken(); // get the actual value

                    if ((sVal != null) && (!sVal.isEmpty())) {

                        int val = Integer.parseInt(sVal);
                        if (val >= 0 && val <= 49) {
                            output += LEVEL_1;
                        } else if (val >= 50 && val <= 99) {
                            output += LEVEL_2;
                        } else if (val >= 100 && val <= 149) {
                            output += LEVEL_3;
                        } else if (val >= 150 && val <= 199) {
                            output += LEVEL_4;
                        } else if (val >= 200 && val <= 299) {
                            output += LEVEL_5;
                        } else if (val >= 300) {
                            output += LEVEL_6;
                        } else if (val == -1) {
                            output += LEVEL_0;
                        }
                    }

                }
                context.write(new Text(ym), new Text(key.toString() + ":" + output + ":" + filename));

            }


        }
    }

    public static class AirQualityReducer1
            extends Reducer<Text, Text, Text, Text> {

        private String new_key = new String();
        private String output = new String();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

             /*
            Re-order (in ascending) according do the offset -> get the corresponding day (also in ascending) -> append day to the year month -> print to key on reducer side
             */
            Map<Integer, String> mapA = new TreeMap<>();
            Map<Integer, String> mapB = new TreeMap<>();

            for (Text val : values) {
                String pre_output = val.toString();
                String[] arr = pre_output.split(":");

                String v_offset = arr[0];
                int v_offset_int = Integer.parseInt(v_offset);
                String out = arr[1];
                String filename = arr[2]; // filename indicator

                if (filename.equals("ShamShuiPov2.txt"))
                    mapA.put(v_offset_int, out);
                else
                    mapB.put(v_offset_int, out);

            }

            List<Integer> offsetByKeyA = new ArrayList<>(mapA.keySet());
            Collections.sort(offsetByKeyA);
            List<Integer> offsetByKeyB = new ArrayList<>(mapB.keySet());
            Collections.sort(offsetByKeyB);

//            for (int i = 0; i < offsetByKeyA.size(); i++) {
//                new_key = key.toString() + ":" + (i + 1);
//                output = mapA.get(offsetByKeyA.get(i));
//                context.write(new Text(new_key), new Text(output));
//            }
//
//            for (int i = 0; i < offsetByKeyB.size(); i++) {
//                new_key = key.toString() + ":" + (i + 1);
//                output = mapB.get(offsetByKeyB.get(i));
//                context.write(new Text(new_key), new Text(output));
//            }
            /* Counter for DIS_1 to DIS_4 */
            int count0 = 0;
            int count1 = 0;
            int count2 = 0;
            int count3 = 0;
            int count4 = 0;

            for (int i = 0; i < offsetByKeyB.size(); i++) {
//                String orderDayA = key.toString() + ":" + (i + 1);
//                String result = "";
                new_key = key.toString() + ":" + (i + 1);
                String outputA = mapA.get(offsetByKeyA.get(i));
                String outputB = mapB.get(offsetByKeyB.get(i));

                /* Calculation on array value difference */
                String[] hourA = outputA.split(" ");
                String[] hourB = outputB.split(" ");
                for (int j = 0; j < 24; j++) {

                    if ((hourA[j] != null) && (!hourA[j].isEmpty()) && (hourB[j] != null) && (!hourB[j].isEmpty())) {
                        int a = Integer.parseInt(hourA[j]);
                        int b = Integer.parseInt(hourB[j]);

                        if (a == -1 || b == -1) {
                            count3++;
                            count4++;
                            continue;
                        } else {
                            int diff = a - b;
                            count4++;
                            if (Math.abs(diff) == 0) {
                                count0++;
                                continue;
                            }
                            if (Math.abs(diff) == 1) {
                                count1++;
                                continue;
                            }
                            if (Math.abs(diff) >= 2) {
                                count2++;
                                continue;
                            }
//                            switch (Math.abs(diff)) {
//                                case 0:
//                                    count0++;
//                                    break;
//                                case 1:
//                                    count1++;
//                                    break;
//                                default:
//                                    count2++;
//                                    break;
//                            }
                        }

                    }

                }
//                context.write(new Text(new_key), new Text(result));
            }

            Map<String, Integer> finalMap = new TreeMap<>();

            finalMap.put(DIS_0, count0);
            finalMap.put(DIS_1, count1);
            finalMap.put(DIS_3, count3);
            finalMap.put(DIS_2, count2);
            finalMap.put(DIS_4, count4);

            List<String> keySet = new ArrayList<>(finalMap.keySet());
            for (int x = 0; x < keySet.size(); x++) {
                context.write(new Text(keySet.get(x)), new Text(finalMap.get(keySet.get(x)).toString()));
            }

        }

    }

    public static class AirQualityMapper2
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] kv = value.toString().split("\t");
            String level = kv[0];
            String count = kv[1];
            context.write(new Text(level), new Text(count));

        }
    }

    public static class AirQualityReducer2
            extends Reducer<Text, Text, Text, Text> {

        private int sum = 0; // total count add together

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                sum += Integer.valueOf(val.toString());
            }
            context.write(key, new Text(Integer.toString(sum)));

        }
    }

    public static void main(String[] args) throws Exception {

        /* JOB1 */
        Configuration conf1 = new Configuration();
        String[] otherArgs1 = new GenericOptionsParser(conf1, args).getRemainingArgs();
        if (otherArgs1.length < 2) {
            System.err.println("Usage: AirQuality <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job1 = Job.getInstance(conf1, "Air Quality Sorting 1 by 16251164");

        //To force machines in Hadoop to use the jar on the HDFS
        //Assume your jar name is AirQuality.jar
        //Please change "bchoi" in "/home/comp/bchoi/AirQuality.jar" to your a/c name
        // also ensure that you have uploaded your jar to HDFS:
        // hadoop fs -put AirQuality.jar
        job1.addFileToClassPath(new Path("/home/comp/e6251164/AirQualityMonitor2.jar"));

        job1.setJarByClass(AirQualityMonitor2.class);
        job1.setMapperClass(AirQualityMapper1.class);
        job1.setReducerClass(AirQualityReducer1.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.setInputDirRecursive(job1, true);
        for (int i = 0; i < otherArgs1.length - 1; ++i) {
            FileInputFormat.addInputPath(job1, new Path(otherArgs1[i]));
        }
        Path outputPath = new Path(otherArgs1[otherArgs1.length - 1]);
        FileOutputFormat.setOutputPath(job1, outputPath);
        job1.waitForCompletion(true);

        /* JOB2 */
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Air Quality Sorting 2 by 16251164, Sherry");

        job2.setJarByClass(AirQualityMonitor2.class);
        job2.setMapperClass(AirQualityMapper2.class);
        job2.setReducerClass(AirQualityReducer2.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.setInputDirRecursive(job2, true);
        FileInputFormat.addInputPath(job2, new Path(otherArgs1[otherArgs1.length - 1]));

        FileOutputFormat.setOutputPath(job2, new Path("/home/comp/e6251164/finalS"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
