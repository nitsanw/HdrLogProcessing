import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.util.Locale;

public class HdrToCsv
{
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

    private File inputFile;

    public static void main(String[] args)
    {
        HdrToCsv app = new HdrToCsv();
        CmdLineParser parser = new CmdLineParser(app);
        try
        {
            parser.parseArgument(args);
            app.execute();
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
            parser.printUsage(System.out);
        }
    }

    private void execute() throws FileNotFoundException
    {
        HistogramLogReader reader = new HistogramLogReader(inputFile);
        System.out.println(
                "#Timestamp,Throughput,Min,Avg,p50,p90,p95,p99,p999,p9999,Max");
        while (reader.hasNext())
        {
            Histogram interval = (Histogram) reader.nextIntervalHistogram();
            System.out.printf(Locale.US,
                    "%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
                    interval.getStartTimeStamp() / 1000.0,
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