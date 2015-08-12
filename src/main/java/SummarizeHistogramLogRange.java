import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
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

    @Option(name="-s",usage="relative log start time in seconds, defaults to 0.0", required=false)
    double start = 0.0;
    @Option(name="-e",usage="relative log end time in seconds, defaults to MAX_DOUBLE", required=false)
    double end = Double.MAX_VALUE;

    private File inputPath = new File(".");
    Set<File> inputFiles = new HashSet<>();
    @Option(name="-v",usage="verbose logging, defaults to false", required=false)
    boolean verbose = false;

	public static void main(String[] args) throws Exception {
	    SummarizeHistogramLogRange app = new SummarizeHistogramLogRange();
	    CmdLineParser parser = new CmdLineParser(app);
	    try {
            parser.parseArgument(args);
            app.execute();
        } catch (Exception e) {
            e.printStackTrace();
            parser.printUsage(System.out);
        }
	}

	public void execute() throws FileNotFoundException {

        Histogram sum = new Histogram(3);
        long period = 0;
        for (File inputFile : inputFiles) {
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
        System.out.printf("TotalCount=%d\n", sum.getTotalCount());
        System.out.printf("Period(ms)=%d\n", period);
        System.out.printf("Throughput(ops/sec)=%.2f\n", avgThpt);
        System.out.printf("Min=%d\n", sum.getMinValue());
        System.out.printf("Mean=%.2f\n", sum.getMean());
        System.out.printf("50.000ptile=%d\n", sum.getValueAtPercentile(50));
        System.out.printf("90.000ptile=%d\n", sum.getValueAtPercentile(90));
        System.out.printf("99.000ptile=%d\n", sum.getValueAtPercentile(99));
        System.out.printf("99.900ptile=%d\n", sum.getValueAtPercentile(99.9));
        System.out.printf("99.990ptile=%d\n", sum.getValueAtPercentile(99.99));
        System.out.printf("99.999ptile=%d\n", sum.getValueAtPercentile(99.999));
        System.out.printf("Max=%d\n", sum.getMaxValue());
	}
    @Option(name = "-inputPath", aliases={"-ip"}, usage = "set path to use for input files, defaults to current folder", required = false)
    public void setInputPath(String inputFolderName) {
        inputPath = new File(inputFolderName);
        if (!inputPath.exists())
            throw new IllegalArgumentException("inputPath must exist!");
        if (!inputPath.isDirectory())
            throw new IllegalArgumentException("inputPath must be a directory!");
    }
    @Option(name = "-inputFile", aliases={"-if"}, usage = "add an input hdr log, also takes regexp", required = false)
    public void addInputFile(String inputFile) {
        final Predicate<String> predicate = Pattern.compile(inputFile).asPredicate();
        inputFiles.addAll(Arrays.asList(inputPath.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return predicate.test(pathname.getName());
            }
        })));
    }



}
