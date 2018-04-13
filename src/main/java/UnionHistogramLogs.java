import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;
import org.HdrHistogram.HistogramLogWriter;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class UnionHistogramLogs
{

    @Option(name = "-start", aliases = "-s", usage = "relative log start time in seconds, (default: 0.0)", required = false)
    public double start = 0.0;

    @Option(name = "-end", aliases = "-e", usage = "relative log end time in seconds, (default: MAX_DOUBLE)", required = false)
    public double end = Double.MAX_VALUE;

    @Option(name = "-verbose", aliases = "-v", usage = "verbose logging, (default: false)", required = false)
    public boolean verbose = false;

    @Option(name = "-relative", aliases = "-r", usage = "relative timeline merge, (default: false)", required = false)
    public boolean relative = false;
    private File inputPath = new File(".");
    private Set<File> inputFiles = new HashSet<>();
    private Map<File, String> inputFilesTags = new HashMap<>();
    private File outputFile;

    public static void main(String[] args) throws Exception
    {
        UnionHistogramLogs app = new UnionHistogramLogs();
        CmdLineParser parser = new CmdLineParser(app);
        try
        {
            parser.parseArgument(args);
            app.execute();
        }
        catch (CmdLineException e)
        {
            System.out.println(e.getMessage());
            parser.printUsage(System.out);
        }
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
        inputFiles.addAll(Arrays.asList(inputPath.listFiles(pathname ->
        {
            return predicate.test(pathname.getName());
        })));
    }

    @Option(name = "-inputFileAbsolute", aliases = "-ifa", usage = "add an input hdr log file by absolute path, also takes regexp", required = false)
    public void addInputFileAbsolute(String inputFile)
    {
        inputFiles.add(new File(inputFile));
    }

    @Option(name = "-taggedInputFile", aliases = "-tif", usage = "a <tag>=<filename> add an input file, tag all histograms from this file with tag. If histograms have a tag it will be conactanated to file tag.", required = false)
    public void addInputFileAbs(String inputFileNameAndTag)
    {
        String[] args = inputFileNameAndTag.split("=");
        if (args.length != 2)
        {
            throw new IllegalArgumentException("This value:" + inputFileNameAndTag +
                " should be a <tag>=<file>, neither tag nor filename allow the '=' char");
        }
        String tag = args[0];
        String inputFileName = args[1];
        File in = new File(inputFileName);
        if (!in.exists())
        {
            throw new IllegalArgumentException("file:" + inputFileName + " must exist!");
        }
        inputFiles.add(in);
        inputFilesTags.put(in, tag);
    }

    @Option(name = "-outputFile", aliases = "-of", usage = "set an output file destination, default goes to sysout", required = false)
    public void setOutputFile(String outputFileName)
    {
        outputFile = new File(outputFileName);
    }

    private void execute() throws FileNotFoundException
    {
        if (verbose)
        {
            if (end != Double.MAX_VALUE)
            {
                System.out.printf("start:%.2f end:%.2f path:%s\n", start, end, inputPath.getAbsolutePath());
            }
            else
            {
                System.out.printf("start:%.2f end: MAX path:%s\n", start, inputPath.getAbsolutePath());
            }

            if (!inputFiles.isEmpty())
            {
                System.out.println("Reading files:");
            }
            else
            {
                System.out.println("No input files :(");
            }

            for (File inputFile : inputFiles)
            {
                System.out.println(inputFile.getAbsolutePath());
            }
        }
        if (inputFiles.isEmpty())
        {
            return;
        }

        PrintStream report = System.out;

        if (outputFile != null)
        {
            report = new PrintStream(new FileOutputStream(outputFile));
        }
        union(report);
    }

    private void union(PrintStream out) throws FileNotFoundException
    {
        List<HistogramIterator> ins = new ArrayList<>();
        for (File inputFile : inputFiles)
        {
            ins.add(new HistogramIterator(new HistogramLogReader(inputFile), inputFilesTags.get(inputFile)));
        }

        ins.removeIf(e -> !e.hasNext());
        Collections.sort(ins);

        if (ins.isEmpty())
        {
            if (verbose)
            {
                System.out.println("Input files do not contain range");
            }
            return;
        }

        // Init writer
        HistogramLogWriter writer = new HistogramLogWriter(out);
        writer.outputLogFormatVersion();
        writer.outputComment("Union of:" + inputFiles + " start:" + start + " end:" + end + " relative:" + relative);
        if (!relative)
        {
            long startTimeStamp = (long) (ins.get(0).reader.getStartTimeSec() * 1000);
            writer.setBaseTime(startTimeStamp);
            writer.outputBaseTime(startTimeStamp);
            writer.outputStartTime(startTimeStamp);
        }
        writer.outputLegend();

        Map<String, Histogram> unionedByTag = new HashMap<>();
        // iterators are now sorted by start time
        while (!ins.isEmpty())
        {
            HistogramIterator input = ins.get(0);
            Histogram next = input.next();
            Histogram union = unionedByTag.computeIfAbsent(next.getTag(), k ->
            {
                Histogram h = new Histogram(next.getNumberOfSignificantValueDigits());
                h.setTag(k);
                return h;
            });

            long nextStart = next.getStartTimeStamp();
            long nextEnd = next.getEndTimeStamp();
            long unionStart = union.getStartTimeStamp();
            long unionEnd = union.getEndTimeStamp();
            // iterators are sorted, so we know nextStart >= unionStart
            boolean rollover = false;

            if (unionStart == Long.MAX_VALUE)
            {
                union.add(next);
            }
            else if (nextStart < unionEnd && nextEnd <= unionEnd)
            {
                union.add(next);
            }
            else if (nextStart < unionEnd)
            {
                double nLength = nextEnd - nextStart;
                double overlap = (unionEnd - nextStart) / nLength;
                if (overlap > 0.8)
                {
                    union.add(next);
                    // prevent an ever expanding union
                    union.setStartTimeStamp(unionStart);
                    union.setEndTimeStamp(unionEnd);
                }
                else
                {
                    rollover = true;
                }
            }
            else
            {
                rollover = true;
            }
            if (rollover)
            {
                writer.outputIntervalHistogram(union);
                union.reset();
                union.setEndTimeStamp(0L);
                union.setStartTimeStamp(Long.MAX_VALUE);
                union.setTag(next.getTag());
                union.add(next);
            }
            // trim and sort
            ins.removeIf(e -> !e.hasNext());
            Collections.sort(ins);
        }
        // write last hgrms
        for (Histogram u : unionedByTag.values())
        {
            writer.outputIntervalHistogram(u);
        }
    }

    class HistogramIterator implements Comparable<HistogramIterator>
    {
        HistogramLogReader reader;
        Histogram next;
        String tag;

        public HistogramIterator(HistogramLogReader reader)
        {
            this.reader = reader;
            read();
        }

        public HistogramIterator(HistogramLogReader reader, String tag)
        {
            this.reader = reader;
            this.tag = tag;
            read();
        }

        private void read()
        {
            do
            {
                next = (Histogram) reader.nextIntervalHistogram(start, end);
            }
            while (next == null && reader.hasNext());
            if (next == null)
            {
                return;
            }

            // replace start time with a relative one
            if (relative)
            {
                long length = next.getEndTimeStamp() - next.getStartTimeStamp();
                long nextStartTime = (long) (next.getStartTimeStamp() - reader.getStartTimeSec() * 1000);
                next.setStartTimeStamp(nextStartTime);
                next.setEndTimeStamp(next.getStartTimeStamp() + length);
            }
            if (tag != null)
            {
                String nextTag = next.getTag();
                if (nextTag == null)
                {
                    next.setTag(tag);
                }
                else
                {
                    next.setTag(tag + "::" + nextTag);
                }
            }
        }

        Histogram next()
        {
            Histogram c = next;
            read();
            return c;
        }

        boolean hasNext()
        {
            return next != null;
        }

        @Override
        public int compareTo(HistogramIterator o)
        {
            if (!hasNext() && !o.hasNext())
            {
                return 0;
            }
            if (!hasNext())
            {
                return -1;
            }
            if (!o.hasNext())
            {
                return 1;
            }
            return (int) (next.getStartTimeStamp() - o.next.getStartTimeStamp());
        }
    }
}
