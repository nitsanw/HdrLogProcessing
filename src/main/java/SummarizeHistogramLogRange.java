import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class SummarizeHistogramLogRange {

    @Option(name = "-s", usage = "relative log start time in seconds, defaults to 0.0", required = false)
    public double start = 0.0;
    @Option(name = "-e", usage = "relative log end time in seconds, defaults to MAX_DOUBLE", required = false)
    public double end = Double.MAX_VALUE;

    private File inputPath = new File(".");
    Set<File> inputFiles = new HashSet<>();
    @Option(name = "-v", usage = "verbose logging, defaults to false", required = false)
    public boolean verbose = false;
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

    public void execute() throws FileNotFoundException {

        if (verbose) {
            if (end != Double.MAX_VALUE)
                System.out.printf("start:%.2f end:%.2f path:%s\n", start, end, inputPath.getAbsolutePath());
            else
                System.out.printf("start:%.2f end: MAX path:%s\n", start, inputPath.getAbsolutePath());
        }
        if (inputFiles.isEmpty())
            return;
        PrintStream report = System.out;
        if(outputFile != null) {
            report = new PrintStream(new FileOutputStream(outputFile));
        }
        summarizeAndPrint(report);
    }

    private void summarizeAndPrint(PrintStream out) throws FileNotFoundException {
        Histogram sum = new Histogram(3);
        long period = 0;
        for (File inputFile : inputFiles) {
            if (verbose)
                System.out.println("Summarizing file:" + inputFile.getName());
            HistogramLogReader reader = new HistogramLogReader(inputFile);
            Histogram interval;
            while ((interval = (Histogram) reader.nextIntervalHistogram(start, end)) != null) {
                sum.add(interval);
            }
            long sumPeriod = (sum.getEndTimeStamp() - sum.getStartTimeStamp());
            sum.setEndTimeStamp(0);
            sum.setStartTimeStamp(Long.MAX_VALUE);
            period += sumPeriod;
        }

        double avgThpt = (sum.getTotalCount() * 1000.0) / period;
        out.printf("TotalCount=%d\n", sum.getTotalCount());
        out.printf("Period(ms)=%d\n", period);
        out.printf("Throughput(ops/sec)=%.2f\n", avgThpt);
        out.printf("Min=%d\n", sum.getMinValue());
        out.printf("Mean=%.2f\n", sum.getMean());
        out.printf("50.000ptile=%d\n", sum.getValueAtPercentile(50));
        out.printf("90.000ptile=%d\n", sum.getValueAtPercentile(90));
        out.printf("99.000ptile=%d\n", sum.getValueAtPercentile(99));
        out.printf("99.900ptile=%d\n", sum.getValueAtPercentile(99.9));
        out.printf("99.990ptile=%d\n", sum.getValueAtPercentile(99.99));
        out.printf("99.999ptile=%d\n", sum.getValueAtPercentile(99.999));
        out.printf("Max=%d\n", sum.getMaxValue());
    }

    @Option(name = "-inputPath", aliases = {"-ip" }, usage = "set path to use for input files, defaults to current folder", required = false)
    public void setInputPath(String inputFolderName) {
        inputPath = new File(inputFolderName);
        if (!inputPath.exists())
            throw new IllegalArgumentException("inputPath:"+inputFolderName+" must exist!");
        if (!inputPath.isDirectory())
            throw new IllegalArgumentException("inputPath:"+inputFolderName+" must be a directory!");
    }

    @Option(name = "-inputFile", aliases = {"-if" }, usage = "add an input hdr log from input path, also takes regexp", required = false)
    public void addInputFile(String inputFile) {
        final Predicate<String> predicate = Pattern.compile(inputFile).asPredicate();
        inputFiles.addAll(Arrays.asList(inputPath.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return predicate.test(pathname.getName());
            }
        })));
    }
    @Option(name = "-inputFilePath", aliases = {"-ifp" }, usage = "add an input file by path relative to working dir or absolute", required = false)
    public void addInputFileAbs(String inputFileName) {
        File in = new File(inputFileName);
        if (!in.exists())
            throw new IllegalArgumentException("file:"+inputFileName+" must exist!");
        inputFiles.add(in);
    }

    @Option(name = "-outputFile", aliases = {"-of" }, usage = "set an output file destination, default goes to sysout", required = false)
    public void setOutputFile(String outputFileName) {
        outputFile = new File(outputFileName);
    }

}
