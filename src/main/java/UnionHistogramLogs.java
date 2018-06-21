import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.kohsuke.args4j.Option;
import psy.lob.saw.HistogramIterator;
import psy.lob.saw.HistogramSink;
import psy.lob.saw.OrderedHistogramLogReader;
import psy.lob.saw.UnionHistograms;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static psy.lob.saw.HdrHistogramUtil.createLogWriter;

public class UnionHistogramLogs implements Runnable
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

    public static void main(String[] args)
    {
        ParseAndRunUtil.parseParamsAndRun(args, new UnionHistogramLogs());
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

    @Override
    public void run()
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
                System.out.println("No input files!");
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


        try
        {
            final PrintStream report;
            if (outputFile != null)
            {
                report = new PrintStream(new FileOutputStream(outputFile));
            }
            else
            {
                report = System.out;
            }
            List<HistogramIterator> ins = new ArrayList<>();
            for (File inputFile : inputFiles)
            {
                ins.add(new HistogramIterator(
                    new OrderedHistogramLogReader(inputFile, start, end),
                    inputFilesTags.get(inputFile),
                    relative));
            }
            UnionHistograms unionHistograms = new UnionHistograms(verbose, System.out, ins, new HistogramSink()
            {
                HistogramLogWriter writer;

                @Override
                public void startTime(double st)
                {
                    String comment = "Union of:" +
                        inputFiles +
                        " start:" +
                        start +
                        " end:" +
                        end +
                        " relative:" +
                        relative;
                    writer = createLogWriter(report, comment, relative ? 0.0 : st);
                }

                @Override
                public void accept(Histogram h)
                {
                    writer.outputIntervalHistogram(h);
                }
            });
            unionHistograms.run();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

}
