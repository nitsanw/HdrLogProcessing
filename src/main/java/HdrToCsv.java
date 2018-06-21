import org.HdrHistogram.Histogram;
import org.kohsuke.args4j.Option;
import psy.lob.saw.OrderedHistogramLogReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.util.Locale;

public class HdrToCsv implements Runnable
{
    private File inputFile;

    public static void main(String[] args)
    {
        ParseAndRunUtil.parseParamsAndRun(args, new HdrToCsv());
    }

    @Option(name = "--input-file",
        aliases = "-i",
        usage = "Relative or absolute path to the input file to read",
        required = true)
    public void setInputFile(String fileName)
    {
        File in = Paths.get(fileName).toFile();
        if (!in.exists())
        {
            throw new IllegalArgumentException(
                "Input file " + fileName + " does not exist");
        }
        inputFile = in;
    }

    @Override
    public void run()
    {
        OrderedHistogramLogReader reader = null;
        try
        {
            reader = new OrderedHistogramLogReader(inputFile);
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }

        System.out.println(
            "#Absolute timestamp,Relative timestamp,Throughput,Min,Avg,p50,p90,p95,p99,p999,p9999,Max");
        while (reader.hasNext())
        {
            Histogram interval = (Histogram) reader.nextIntervalHistogram();
            System.out.printf(Locale.US,
                "%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
                interval.getStartTimeStamp() / 1000.0,
                interval.getStartTimeStamp() / 1000 - (long) reader.getStartTimeSec(),
                interval.getTotalCount(), interval.getMinValue(),
                (long) interval.getMean(),
                interval.getValueAtPercentile(50),
                interval.getValueAtPercentile(90),
                interval.getValueAtPercentile(95),
                interval.getValueAtPercentile(99),
                interval.getValueAtPercentile(99.9),
                interval.getValueAtPercentile(99.99),
                interval.getMaxValue());
        }
    }
}