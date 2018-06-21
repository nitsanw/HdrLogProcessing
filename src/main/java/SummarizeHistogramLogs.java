import org.HdrHistogram.Histogram;
import org.kohsuke.args4j.Option;
import psy.lob.saw.OrderedHistogramLogReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static psy.lob.saw.HdrHistogramUtil.logHistogramForVerbose;

public class SummarizeHistogramLogs implements Runnable
{

    @Option(name = "-ignoreTag", aliases = "-it", usage = "summary should not be split by tag, (default: false)", required = false)
    public boolean ignoreTag = false;
    @Option(name = "-ignoreTimeStamps", aliases = "-its", usage = "summary should ignore time stamps for period calculation, use interval length instead, (default: false)", required = false)
    public boolean ignoreTimeStamps = false;
    @Option(name = "-start", aliases = "-s", usage = "relative log start time in seconds, (default: 0.0)", required = false)
    public double start = 0.0;
    @Option(name = "-end", aliases = "-e", usage = "relative log end time in seconds, (default: MAX_DOUBLE)", required = false)
    public double end = Double.MAX_VALUE;
    @Option(name = "-verbose", aliases = "-v", usage = "verbose logging, (default: false)", required = false)
    public boolean verbose = false;
    @Option(name = "-summaryType", aliases = "-st", usage = "summary type: csv, percentiles, hgrm", required = false)
    public SummaryType summaryType = SummaryType.PERCENTILES;
    @Option(name = "-percentilesOutputTicksPerHalf", aliases = "-tph", usage = "ticks per half percentile, used for hgrm output, (default: 5)", required = false)
    public int percentilesOutputTicksPerHalf = 5;
    @Option(name = "-outputValueUnitRatio", aliases = "-ovr", usage = "output value unit ratio, (default: 1.0)", required = false)
    public double outputValueUnitRatio = 1.0;
    @Option(name = "-outputBucketSize", aliases = "-obs", usage = "csv output bucket size, (default: 100)", required = false)
    public long outputBucketSize = 100;
    @Option(name = "-outputFile", aliases = "-of", usage = "set an output file destination, default goes to sysout", required = false)
    public String outputFile;
    private File inputPath = new File(".");
    private Set<File> inputFiles = new HashSet<>();

    public static void main(String[] args)
    {
        ParseAndRunUtil.parseParamsAndRun(args, new SummarizeHistogramLogs());
    }

    @Option(name = "-inputPath", aliases = "-ip", usage = "set path to use for input files, defaults to current folder", required = false)
    public void setInputPath(String inputFolderName)
    {
        inputPath = new File(inputFolderName);
        if (!inputPath.exists())
        {
            throw new IllegalArgumentException("inputPath:" + inputFolderName + " must exist!");
        }
        if (!inputPath.isDirectory())
        {
            throw new IllegalArgumentException("inputPath:" + inputFolderName + " must be a directory!");
        }
    }

    @Option(name = "-inputFile", aliases = "-if", usage = "add an input hdr log from input path, also takes regexp", required = false)
    public void addInputFile(String inputFile)
    {
        final Predicate<String> predicate = Pattern.compile(inputFile).asPredicate();
        inputFiles.addAll(
            Arrays.asList(
                inputPath.listFiles(pathname ->
                {
                    return predicate.test(pathname.getName());
                })
            )
        );
    }

    @Option(name = "-inputFilePath", aliases = "-ifp", usage = "add an input file by path relative to working dir or absolute", required = false)
    public void addInputFileAbs(String inputFileName)
    {
        File in = new File(inputFileName);
        if (!in.exists())
        {
            throw new IllegalArgumentException("file:" + inputFileName + " must exist!");
        }
        inputFiles.add(in);
    }

    public void run()
    {
        if (verbose)
        {
            if (end != Double.MAX_VALUE)
            {
                System.out.printf("start:%.2f end:%.2f path:%s%n", start, end, inputPath.getAbsolutePath());
            }
            else
            {
                System.out.printf("start:%.2f end: MAX path:%s%n", start, inputPath.getAbsolutePath());
            }
        }
        if (inputFiles.isEmpty())
        {
            return;
        }

        try
        {
            summarizeAndPrint();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private void summarizeAndPrint() throws FileNotFoundException
    {
        Map<String, Histogram> sumByTag = new HashMap<>();

        long period = 0;
        long intervalLengthSum = 0;
        for (File inputFile : inputFiles)
        {
            if (verbose)
            {
                System.out.println("Summarizing file: " + inputFile.getName());
            }
            OrderedHistogramLogReader reader = new OrderedHistogramLogReader(inputFile, start, end);
            Histogram interval;
            int i = 0;
            boolean first = true;
            long startTime = 0;

            while (reader.hasNext())
            {
                interval = (Histogram) reader.nextIntervalHistogram();
                if (interval == null)
                {
                    continue;
                }
                if (first)
                {
                    first = false;
                    startTime = interval.getStartTimeStamp();
                    if (verbose)
                    {
                        System.out.println("StartTime: " + new Date(startTime));
                    }

                }
                String ntag = ignoreTag ? null : interval.getTag();
                final int numberOfSignificantValueDigits = interval.getNumberOfSignificantValueDigits();
                Histogram sum = sumByTag.computeIfAbsent(ntag, k ->
                {
                    Histogram h = new Histogram(numberOfSignificantValueDigits);
                    h.setTag(k);
                    return h;
                });
                final long intervalLength = interval.getEndTimeStamp() - interval.getStartTimeStamp();
                intervalLengthSum += intervalLength;
                sum.add(interval);
                if (verbose)
                {
                    logHistogramForVerbose(System.out, interval, i++, outputValueUnitRatio);
                }
            }
            // calculate period
            long maxPeriod = 0;
            for (Histogram sum : sumByTag.values())
            {
                long sumPeriod = (sum.getEndTimeStamp() - sum.getStartTimeStamp());
                if (verbose)
                {
                    System.out.print(inputFile.getName());
                    System.out.print(", ");
                    logHistogramForVerbose(System.out, sum, i++, outputValueUnitRatio);
                }
                sum.setEndTimeStamp(0);
                sum.setStartTimeStamp(Long.MAX_VALUE);
                maxPeriod = Math.max(maxPeriod, sumPeriod);
            }
            period += maxPeriod;
        }
        if (ignoreTimeStamps)
        {
            period = intervalLengthSum;
        }
        for (Histogram sum : sumByTag.values())
        {
            String tag = (sum.getTag() == null) ? "" : "." + sum.getTag();
            PrintStream out = getOut(tag);
            switch (summaryType)
            {
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

    private PrintStream getOut(String tag) throws FileNotFoundException
    {
        PrintStream report = System.out;
        if (outputFile != null)
        {
            report = new PrintStream(new FileOutputStream(outputFile + tag + ".hgrm"));
        }
        return report;
    }

    private void printHgrm(PrintStream out, Histogram sum)
    {
        sum.outputPercentileDistribution(out, percentilesOutputTicksPerHalf, outputValueUnitRatio);
    }

    private void printPercentiles(PrintStream out, Histogram sum, long period)
    {
        double avgThpt = (sum.getTotalCount() * 1000.0) / period;
        String tag = (sum.getTag() == null) ? "" : sum.getTag() + ".";
        out.printf("%sTotalCount=%d%n", tag, sum.getTotalCount());
        out.printf("%sPeriod(ms)=%d%n", tag, period);
        out.printf("%sThroughput(ops/sec)=%.2f%n", tag, avgThpt);
        out.printf("%sMin=%d%n", tag, (long) (sum.getMinValue() / outputValueUnitRatio));
        out.printf("%sMean=%.2f%n", tag, sum.getMean() / outputValueUnitRatio);
        out.printf("%s50.000ptile=%d%n", tag, (long) (sum.getValueAtPercentile(50) / outputValueUnitRatio));
        out.printf("%s90.000ptile=%d%n", tag, (long) (sum.getValueAtPercentile(90) / outputValueUnitRatio));
        out.printf("%s99.000ptile=%d%n", tag, (long) (sum.getValueAtPercentile(99) / outputValueUnitRatio));
        out.printf("%s99.900ptile=%d%n", tag, (long) (sum.getValueAtPercentile(99.9) / outputValueUnitRatio));
        out.printf("%s99.990ptile=%d%n", tag, (long) (sum.getValueAtPercentile(99.99) / outputValueUnitRatio));
        out.printf("%s99.999ptile=%d%n", tag, (long) (sum.getValueAtPercentile(99.999) / outputValueUnitRatio));
        out.printf("%sMax=%d%n", tag, (long) (sum.getMaxValue() / outputValueUnitRatio));
    }

    private void printCsv(PrintStream out, Histogram sum)
    {
        long min = (long) (sum.getMinValue() / outputValueUnitRatio);
        long max = (long) (sum.getMaxValue() / outputValueUnitRatio);
        long bucketStart = (min / outputBucketSize) * outputBucketSize;
        out.println("BucketStart, Count");
        for (; bucketStart < max; bucketStart += outputBucketSize)
        {
            long s = (long) (bucketStart * outputValueUnitRatio);
            long e = (long) ((bucketStart + outputBucketSize) * outputValueUnitRatio);
            long count = sum.getCountBetweenValues(s, e);
            out.print(bucketStart);
            out.print(",");
            out.println(count);
        }
    }

    enum SummaryType
    {
        CSV, PERCENTILES, HGRM
    }
}
