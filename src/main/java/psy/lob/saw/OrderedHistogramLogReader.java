/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 *
 * @author Gil Tene
 */

package psy.lob.saw;

import org.HdrHistogram.EncodableHistogram;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.function.Predicate;
import java.util.zip.DataFormatException;

/**
 * Revised {@link org.HdrHistogram.HistogramLogReader} which utilizes the {@link HistogramLogScanner} to support better
 * iteration functionality.
 */
public class OrderedHistogramLogReader implements Closeable
{

    private final HistogramLogScanner scanner;
    private final HistogramLogScanner.EventHandler handler = new HistogramLogScanner.EventHandler()
    {
        @Override
        public boolean onComment(String comment)
        {
//            System.out.println(comment);
            return false;
        }

        @Override
        public boolean onBaseTime(double secondsSinceEpoch)
        {
            baseTimeSec = secondsSinceEpoch; // base time represented as seconds since epoch
            observedBaseTime = true;
            return false;
        }

        @Override
        public boolean onStartTime(double secondsSinceEpoch)
        {
            startTimeSec = secondsSinceEpoch; // start time represented as seconds since epoch
            observedStartTime = true;
            return false;
        }

        @Override
        public boolean onHistogram(
            String tag, double timestamp, double length,
            HistogramLogScanner.EncodableHistogramSupplier lazyReader)
        {
            final double logTimeStampInSec = timestamp; // Timestamp is expected to be in seconds

            if (!observedStartTime)
            {
                // No explicit start time noted. Use 1st observed time:
                startTimeSec = logTimeStampInSec;
                observedStartTime = true;
            }

            if (!observedBaseTime)
            {
                // No explicit base time noted. Deduce from 1st observed time (compared to start time):
                if (logTimeStampInSec < startTimeSec - (365 * 24 * 3600.0))
                {
                    // Criteria Note: if log timestamp is more than a year in the past (compared to
                    // StartTime), we assume that timestamps in the log are not absolute
                    baseTimeSec = startTimeSec;
                }
                else
                {
                    // Timestamps are absolute
                    baseTimeSec = 0.0;
                }
                observedBaseTime = true;
            }

            final double absoluteStartTimeStampSec = logTimeStampInSec + baseTimeSec;
            final double offsetStartTimeStampSec = absoluteStartTimeStampSec - startTimeSec;

            final double intervalLengthSec = length; // Timestamp length is expect to be in seconds
            final double absoluteEndTimeStampSec = absoluteStartTimeStampSec + intervalLengthSec;

            final double startTimeStampToCheckRangeOn = absolute ? absoluteStartTimeStampSec : offsetStartTimeStampSec;

            if (startTimeStampToCheckRangeOn < rangeStartTimeSec)
            {
                return false;
            }

            if (startTimeStampToCheckRangeOn > rangeEndTimeSec)
            {
                // trip the inRange so that readers can stop now
                inRange = false;
                return true;
            }

            // skip excluded
            if (shouldExcludeTag.test(tag))
            {
                return false;
            }

            EncodableHistogram histogram;
            try
            {
                histogram = lazyReader.read();
            }
            catch (DataFormatException e)
            {
                return true;
            }

            histogram.setStartTimeStamp((long) (absoluteStartTimeStampSec * 1000.0));
            histogram.setEndTimeStamp((long) (absoluteEndTimeStampSec * 1000.0));
            histogram.setTag(tag);
            nextHistogram = histogram;
            return true;
        }

        @Override
        public boolean onException(Throwable t)
        {
            t.printStackTrace();
            return false;
        }
    };

    // scanner handling state
    private double startTimeSec = 0.0;
    private boolean observedStartTime = false;
    private double baseTimeSec = 0.0;
    private boolean observedBaseTime = false;

    private final boolean absolute;
    private final double rangeStartTimeSec;
    private final double rangeEndTimeSec;
    private final Predicate<String> shouldExcludeTag;
    private EncodableHistogram nextHistogram;
    private boolean inRange = true;

    public OrderedHistogramLogReader(final File inputFile) throws FileNotFoundException
    {
        this(inputFile, 0.0, Long.MAX_VALUE * 1.0, s -> false, false);
    }

    public OrderedHistogramLogReader(File inputFile, double start, double end) throws FileNotFoundException
    {
        this(inputFile, start, end, s -> false, false);
    }

    public OrderedHistogramLogReader(File inputFile, double start, double end, Predicate<String> shouldExcludeTag)
        throws FileNotFoundException
    {
        this(inputFile, start, end, shouldExcludeTag, false);
    }

    /**
     * Constructs a new OrderedHistogramLogReader that produces intervals read from the specified file.
     *
     * @param inputFile         The File to read from
     * @param rangeStartTimeSec
     * @param rangeEndTimeSec
     * @param shouldExcludeTag  predicate returns true is tag should be skipped
     * @param absolute
     * @throws FileNotFoundException when unable to find inputFile
     */
    public OrderedHistogramLogReader(
        final File inputFile,
        double rangeStartTimeSec,
        double rangeEndTimeSec,
        Predicate<String> shouldExcludeTag, boolean absolute) throws FileNotFoundException
    {
        scanner = new HistogramLogScanner(inputFile);
        this.rangeStartTimeSec = rangeStartTimeSec;
        this.rangeEndTimeSec = rangeEndTimeSec;
        this.absolute = absolute;
        this.shouldExcludeTag = shouldExcludeTag;
    }

    /**
     * get the latest start time found in the file so far (or 0.0),
     * per the log file format explained above. Assuming the "#[StartTime:" comment
     * line precedes the actual intervals recorded in the file, getStartTimeSec() can
     * be safely used after each interval is read to determine's the offset of that
     * interval's timestamp from the epoch.
     *
     * @return latest Start Time found in the file (or 0.0 if non found)
     */
    public double getStartTimeSec()
    {
        return startTimeSec;
    }

    /**
     * Read the next interval histogram from the log. Returns a Histogram object if
     * an interval line was found, or null if not.
     * <p>Upon encountering any unexpected format errors in reading the next interval
     * from the input, this method will return a null. Use {@link #hasNext} to determine
     * whether or not additional intervals may be available for reading in the log input.
     *
     * @return a DecodedInterval, or a null if no appropriately formatted interval was found
     */
    public EncodableHistogram nextIntervalHistogram()
    {

        scanner.process(handler);
        EncodableHistogram histogram = this.nextHistogram;
        nextHistogram = null;
        return histogram;
    }

    /**
     * Indicates whether or not additional intervals may exist in the log
     *
     * @return true if additional intervals may exist in the log
     */
    public boolean hasNext()
    {
        return scanner.hasNextLine() && inRange;
    }

    @Override
    public void close()
    {
        scanner.close();
    }
}
