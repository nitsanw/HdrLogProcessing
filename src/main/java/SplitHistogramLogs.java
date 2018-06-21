import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.kohsuke.args4j.Option;
import psy.lob.saw.HdrHistogramUtil;
import psy.lob.saw.OrderedHistogramLogReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static psy.lob.saw.HdrHistogramUtil.logHistogramForVerbose;

public class SplitHistogramLogs implements Runnable
{
    @Option(name = "-start", aliases = "-s", usage = "relative log start time in seconds, (default: 0.0)", required = false)
    public double start = 0.0;

    @Option(name = "-end", aliases = "-e", usage = "relative log end time in seconds, (default: MAX_DOUBLE)", required = false)
    public double end = Double.MAX_VALUE;

    @Option(name = "-verbose", aliases = "-v", usage = "verbose logging, (default: false)", required = false)
    public boolean verbose = false;
    private File inputPath = new File(".");
    private File inputFile;
    private Set<String> excludeTags = new HashSet<>();
    private Set<String> includeTags = new HashSet<>();

    public static void main(String[] args) throws Exception
    {
        ParseAndRunUtil.parseParamsAndRun(args, new SplitHistogramLogs());
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

    @Option(name = "-inputFile", aliases = "-if", usage = "set the input hdr log from input path", required = true)
    public void setInputFile(String inputFileName)
    {
        inputFile = new File(inputPath, inputFileName);
        if (!inputFile.exists())
        {

            inputFile = new File(inputFileName);
            if (!inputFile.exists())
            {
                throw new IllegalArgumentException("inputFile:" + inputFileName + " must exist!");
            }
        }

    }

    @Option(name = "-excludeTag", aliases = "-et", usage = "add a tag to filter from input, 'default' is a special tag for the null tag.", required = false)
    public void addExcludeTag(String tag)
    {
        excludeTags.add(tag);
    }

    @Option(name = "-includeTag", aliases = "-it", usage = "when include tags are used only the explicitly included will be split out, 'default' is a special tag for the null tag.", required = false)
    public void addIncludeTag(String tag)
    {
        includeTags.add(tag);
    }

    @Override
    public void run()
    {
        if (verbose)
        {
            String absolutePath = inputPath.getAbsolutePath();
            String name = inputFile.getName();
            if (end != Double.MAX_VALUE)
            {
                System.out.printf("start:%.2f end:%.2f path:%s file:%s \n", start, end, absolutePath, name);
            }
            else
            {
                System.out.printf("start:%.2f end: MAX path:%s file:%s \n", start, absolutePath, name);
            }
        }
        try
        {
            split();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private void split() throws FileNotFoundException
    {
        OrderedHistogramLogReader reader = new OrderedHistogramLogReader(
            inputFile,
            start,
            end,
            tag -> shouldSkipTag(tag));
        Map<String, HistogramLogWriter> writerByTag = new HashMap<>();
        Histogram interval;
        int i = 0;
        while (reader.hasNext())
        {
            interval = (Histogram) reader.nextIntervalHistogram();
            if (interval == null)
            {
                continue;
            }
            String ntag = interval.getTag();
            if (shouldSkipTag(ntag))
            {
                throw new IllegalStateException("Should be filtered upfront by the reader");
            }
            if (verbose)
            {
                logHistogramForVerbose(System.out, interval, i++);
            }
            interval.setTag(null);
            HistogramLogWriter writer = writerByTag.computeIfAbsent(ntag, k -> createWriterForTag(reader, k));
            writer.outputIntervalHistogram(interval);

        }
    }

    private boolean shouldSkipTag(String ntag)
    {
        ntag = (ntag == null) ? "default" : ntag;
        return excludeTags.contains(ntag) || (!includeTags.isEmpty() && !includeTags.contains(ntag));
    }

    private HistogramLogWriter createWriterForTag(OrderedHistogramLogReader reader, String tag)
    {
        tag = (tag == null) ? "default" : tag;
        File outputFile = new File(tag + "." + inputFile.getName());
        String comment = "Splitting of:" + inputFile.getName() + " start:" + start + " end:" + end;
        HistogramLogWriter writer = HdrHistogramUtil.createLogWriter(outputFile, comment, reader.getStartTimeSec());
        return writer;
    }
}
