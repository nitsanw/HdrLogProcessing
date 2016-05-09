import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class SummarizeHistogramLogRange {

    enum SummaryType {
        CSV, PERCENTILES, HGRM
    }

    @Option(name="-ignoreTag", aliases="-it", usage="summary should not be split by tag, (default: false)", required=false)
    public boolean ignoreTag = false;

    @Option(name="-start", aliases="-s", usage="relative log start time in seconds, (default: 0.0)", required=false)
    public double start = 0.0;

    @Option(name="-end", aliases="-e", usage="relative log end time in seconds, (default: MAX_DOUBLE)", required=false)
    public double end = Double.MAX_VALUE;

    @Option(name="-verbose", aliases="-v", usage="verbose logging, (default: false)", required=false)
    public boolean verbose = false;

    @Option(name="-summaryType", aliases="-st", usage="summary type: csv, percentiles, hgrm", required=false)
    public SummaryType summaryType = SummaryType.PERCENTILES;

    @Option(name="-percentilesOutputTicksPerHalf", aliases="-tph", usage="ticks per half percentile, used for hgrm output, (default: 5)", required=false)
    public int percentilesOutputTicksPerHalf = 5;

    @Option(name="-outputValueUnitRatio", aliases="-ovr", usage="output value unit ratio, (default: 1.0)", required=false)
    public double outputValueUnitRatio = 1.0;

    @Option(name="-outputBucketSize", aliases="-obs", usage="csv output bucket size, (default: 100)", required=false)
    public long outputBucketSize = 100;

    @Option(name="-inputPath", aliases="-ip", usage="set path to use for input files, defaults to current folder", required=false)
    public void setInputPath(String inputFolderName) {
        inputPath = new File(inputFolderName);
        if (!inputPath.exists())
            throw new IllegalArgumentException("inputPath:" + inputFolderName + " must exist!");
        if (!inputPath.isDirectory())
            throw new IllegalArgumentException("inputPath:" + inputFolderName + " must be a directory!");
    }

    @Option(name="-inputFile", aliases="-if", usage="add an input hdr log from input path, also takes regexp", required=false)
    public void addInputFile(String inputFile) {
        final Predicate<String> predicate = Pattern.compile(inputFile).asPredicate();
        inputFiles.addAll(
          Arrays.asList(
            inputPath.listFiles(pathname -> {return predicate.test(pathname.getName());})
          )
        );
    }

    @Option(name="-inputFilePath", aliases="-ifp", usage="add an input file by path relative to working dir or absolute", required=false)
    public void addInputFileAbs(String inputFileName) {
        File in = new File(inputFileName);
        if (!in.exists())
            throw new IllegalArgumentException("file:" + inputFileName + " must exist!");
        inputFiles.add(in);
    }

    @Option(name="-outputFile", aliases="-of", usage="set an output file destination, default goes to sysout", required=false)
    public void setOutputFile(String outputFileName) {
        outputFile = new File(outputFileName);
    }


    private File inputPath = new File(".");
    private Set<File> inputFiles = new HashSet<>();
    private File outputFile;

    public static void main(String[] args) throws Exception {
        SummarizeHistogramLogRange app = new SummarizeHistogramLogRange();
        CmdLineParser parser = new CmdLineParser(app);
        try {
            parser.parseArgument(args);
            app.execute();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            parser.printUsage(System.out);
        }
    }

    private void execute() throws FileNotFoundException {
        if (verbose) {
            if (end != Double.MAX_VALUE)
                System.out.printf("start:%.2f end:%.2f path:%s\n", start, end, inputPath.getAbsolutePath());
            else
                System.out.printf("start:%.2f end: MAX path:%s\n", start, inputPath.getAbsolutePath());
        }
        if (inputFiles.isEmpty())
            return;
        PrintStream report = System.out;
        if (outputFile != null) {
            report = new PrintStream(new FileOutputStream(outputFile));
        }
        summarizeAndPrint(report);
    }

    private void summarizeAndPrint(PrintStream out) throws FileNotFoundException {
        Map<String, Histogram> sumByTag= new HashMap<>();

        long period = 0;
        for (File inputFile : inputFiles) {
            if (verbose)
                System.out.println("Summarizing file: " + inputFile.getName());
            HistogramLogReader reader = new HistogramLogReader(inputFile);
            Histogram interval;
            int i = 0;
            while ((interval = (Histogram) reader.nextIntervalHistogram(start, end)) != null) {
                String ntag = ignoreTag ? null : interval.getTag();
                Histogram sum = sumByTag.computeIfAbsent(ntag, k -> {Histogram h = new Histogram(3); h.setTag(k); return h;});
                sum.add(interval);
                if (verbose) {
                    String tag = (sum.getTag() == null) ? "" : "("+sum.getTag()+") ";
                    System.out.printf("%s%d: [count=%d,min=%d,max=%d,avg=%.2f,50=%d,99=%d,999=%d]\n",
                            tag, i++,
                            interval.getTotalCount(),
                            (long) (interval.getMinValue() / outputValueUnitRatio),
                            (long) (interval.getMaxValue() / outputValueUnitRatio),
                            interval.getMean() / outputValueUnitRatio,
                            (long) (interval.getValueAtPercentile(50) / outputValueUnitRatio),
                            (long) (interval.getValueAtPercentile(99) / outputValueUnitRatio),
                            (long) (interval.getValueAtPercentile(99.9) / outputValueUnitRatio));
                }
            }
            long maxPeriod = 0;
            for (Histogram sum : sumByTag.values()) {
                long sumPeriod = (sum.getEndTimeStamp() - sum.getStartTimeStamp());
                if (verbose) {
                    String tag = (sum.getTag() == null) ? "" : "::"+sum.getTag();
                    System.out.printf("%s%s %d secs [count=%d,min=%d,max=%d,avg=%.2f,50=%d,99=%d,999=%d]\n",
                            inputFile.getName(), tag,
                            sumPeriod / 1000, sum.getTotalCount(),
                            (long) (sum.getMinValue() / outputValueUnitRatio),
                            (long) (sum.getMaxValue() / outputValueUnitRatio), sum.getMean() / outputValueUnitRatio,
                            (long) (sum.getValueAtPercentile(50) / outputValueUnitRatio),
                            (long) (sum.getValueAtPercentile(99) / outputValueUnitRatio),
                            (long) (sum.getValueAtPercentile(99.9) / outputValueUnitRatio));
                }
                sum.setEndTimeStamp(0);
                sum.setStartTimeStamp(Long.MAX_VALUE);
                maxPeriod = Math.max(maxPeriod, sumPeriod);
            }
            period += maxPeriod;
        }
        for (Histogram sum : sumByTag.values()) {
            switch (summaryType) {
            case PERCENTILES:
                printPercentiles(out, sum, period);
                break;
            case CSV:
                printCsv(out, sum);
                break;
            case HGRM:
                printHgrm(out, sum);
                break;
            default:
                throw new IllegalStateException();
            }
        }
    }

    private void printHgrm(PrintStream out, Histogram sum) {
        sum.outputPercentileDistribution(out, percentilesOutputTicksPerHalf, outputValueUnitRatio);
    }

    private void printPercentiles(PrintStream out, Histogram sum, long period) {
        double avgThpt = (sum.getTotalCount() * 1000.0) / period;
        String tag = (sum.getTag() == null) ? "" : sum.getTag() + ".";
        out.printf("%sTotalCount=%d\n", tag, sum.getTotalCount());
        out.printf("%sPeriod(ms)=%d\n", tag, period);
        out.printf("%sThroughput(ops/sec)=%.2f\n", tag, avgThpt);
        out.printf("%sMin=%d\n", tag, (long) (sum.getMinValue() / outputValueUnitRatio));
        out.printf("%sMean=%.2f\n", tag, sum.getMean() / outputValueUnitRatio);
        out.printf("%s50.000ptile=%d\n", tag, (long) (sum.getValueAtPercentile(50) / outputValueUnitRatio));
        out.printf("%s90.000ptile=%d\n", tag, (long) (sum.getValueAtPercentile(90) / outputValueUnitRatio));
        out.printf("%s99.000ptile=%d\n", tag, (long) (sum.getValueAtPercentile(99) / outputValueUnitRatio));
        out.printf("%s99.900ptile=%d\n", tag, (long) (sum.getValueAtPercentile(99.9) / outputValueUnitRatio));
        out.printf("%s99.990ptile=%d\n", tag, (long) (sum.getValueAtPercentile(99.99) / outputValueUnitRatio));
        out.printf("%s99.999ptile=%d\n", tag, (long) (sum.getValueAtPercentile(99.999) / outputValueUnitRatio));
        out.printf("%sMax=%d\n", tag, (long) (sum.getMaxValue() / outputValueUnitRatio));
    }

    private void printCsv(PrintStream out, Histogram sum) {
        long min = (long) (sum.getMinValue() / outputValueUnitRatio);
        long max = (long) (sum.getMaxValue() / outputValueUnitRatio);
        long bucketStart = (min / outputBucketSize) * outputBucketSize;
        out.println("BucketStart, Count");
        for (; bucketStart < max; bucketStart += outputBucketSize) {
            long s = (long) (bucketStart * outputValueUnitRatio);
            long e = (long) ((bucketStart + outputBucketSize) * outputValueUnitRatio);
            long count = sum.getCountBetweenValues(s, e);
            out.print(bucketStart);
            out.print(",");
            out.println(count);
        }
    }
}
