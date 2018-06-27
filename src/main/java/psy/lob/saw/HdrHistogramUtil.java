package psy.lob.saw;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.io.PrintStream;

public class HdrHistogramUtil
{
    public static void logHistogramForVerbose(PrintStream verboseOut, Histogram interval, int i)
    {
        logHistogramForVerbose(verboseOut, interval, i, 1);
    }

    public static void logHistogramForVerbose(
        PrintStream verboseOut,
        Histogram interval,
        int i,
        double outputValueUnitRatio)
    {
        String tag = (interval.getTag() == null) ? "default" : interval.getTag();
        double intervalLengthSec = (interval.getEndTimeStamp() - interval.getStartTimeStamp())/1000.0;
        verboseOut.printf("%s %5d: (%8.3f to %8.3f) [count=%d,min=%d,max=%d,avg=%.2f,50=%d,99=%d,999=%d,ops/s=%.1f]%n",
            tag, i,
            interval.getStartTimeStamp() / 1000.0,
            interval.getEndTimeStamp() / 1000.0,
            interval.getTotalCount(),
            (long) (interval.getMinValue() / outputValueUnitRatio),
            (long) (interval.getMaxValue() / outputValueUnitRatio),
            interval.getMean() / outputValueUnitRatio,
            (long) (interval.getValueAtPercentile(50) / outputValueUnitRatio),
            (long) (interval.getValueAtPercentile(99) / outputValueUnitRatio),
            (long) (interval.getValueAtPercentile(99.9) / outputValueUnitRatio),
            interval.getTotalCount() / intervalLengthSec);
    }

    public static HistogramLogWriter createLogWriter(File output, String comment, double startTimeSec)
    {
        try
        {
            return createLogWriter(new HistogramLogWriter(output), comment, startTimeSec);
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static HistogramLogWriter createLogWriter(OutputStream output, String comment, double startTimeSec)
    {
        return createLogWriter(new HistogramLogWriter(output), comment, startTimeSec);
    }

    public static HistogramLogWriter createLogWriter(PrintStream output, String comment, double startTimeSec)
    {
        return createLogWriter(new HistogramLogWriter(output), comment, startTimeSec);
    }

    private static HistogramLogWriter createLogWriter(
        HistogramLogWriter writer,
        String comment,
        double startTimeSec)
    {
        writer.outputLogFormatVersion();
        if (comment != null)
        {
            writer.outputComment(comment);
        }
        if (startTimeSec != 0.0)
        {
            long startTimeStamp = (long) (startTimeSec * 1000);
            writer.setBaseTime(startTimeStamp);
            writer.outputBaseTime(startTimeStamp);
            writer.outputStartTime(startTimeStamp);
        }
        writer.outputLegend();
        return writer;
    }
}
