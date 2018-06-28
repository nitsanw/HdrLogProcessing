/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 *
 * @author Gil Tene
 */
package psy.lob.saw;

import org.HdrHistogram.DoubleHistogram;
import org.HdrHistogram.EncodableHistogram;
import org.HdrHistogram.Histogram;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Locale;
import java.util.Scanner;
import java.util.zip.DataFormatException;

/**
 * A histogram log reader.
 * <p>
 * Histogram logs are used to capture full fidelity, per-time-interval
 * histograms of a recorded value.
 * <p>
 * For example, a histogram log can be used to capture high fidelity
 * reaction-time logs for some measured system or subsystem component.
 * Such a log would capture a full reaction time histogram for each
 * logged interval, and could be used to later reconstruct a full
 * HdrHistogram of the measured reaction time behavior for any arbitrary
 * time range within the log, by adding [only] the relevant interval
 * histograms.
 * <h3>Histogram log format:</h3>
 * A histogram log file consists of text lines. Lines beginning with
 * the "#" character are optional and treated as comments. Lines
 * containing the legend (starting with "Timestamp") are also optional
 * and ignored in parsing the histogram log. All other lines must
 * be valid interval description lines. Text fields are delimited by
 * commas, spaces.
 * <p>
 * A valid interval description line contains an optional Tag=tagString
 * text field, followed by an interval description.
 * <p>
 * A valid interval description must contain exactly four text fields:
 * <ul>
 * <li>StartTimestamp: The first field must contain a number parse-able as a Double value,
 * representing the start timestamp of the interval in seconds.</li>
 * <li>intervalLength: The second field must contain a number parse-able as a Double value,
 * representing the length of the interval in seconds.</li>
 * <li>Interval_Max: The third field must contain a number parse-able as a Double value,
 * which generally represents the maximum value of the interval histogram.</li>
 * <li>Interval_Compressed_Histogram: The fourth field must contain a text field
 * parse-able as a Base64 text representation of a compressed HdrHistogram.</li>
 * </ul>
 * The log file may contain an optional indication of a starting time. Starting time
 * is indicated using a special comments starting with "#[StartTime: " and followed
 * by a number parse-able as a double, representing the start time (in seconds)
 * that may be added to timestamps in the file to determine an absolute
 * timestamp (e.g. since the epoch) for each interval.
 */
public class HistogramLogScanner implements Closeable
{

    // can't use lambdas, and anyway we need to let the handler take the exception
    public interface EncodableHistogramSupplier
    {
        EncodableHistogram read() throws DataFormatException;
    }

    /**
     * Handles log events, return true to stop processing.
     */
    public interface EventHandler
    {
        /**
         * @param comment a non-standard comment observed in the log, e.g. "#Our's is a nice 'ouse, our's is, We've got no rats or mouses"
         * @return false to keep processing, true to stop
         */
        boolean onComment(String comment);

        /**
         * @param secondsSinceEpoch observed standard comment tag: "# BaseTime: "
         * @return false to keep processing, true to stop
         */
        boolean onBaseTime(double secondsSinceEpoch);

        /**
         * @param secondsSinceEpoch observed standard comment tag: "# StartTime: "
         * @return false to keep processing, true to stop
         */
        boolean onStartTime(double secondsSinceEpoch);

        /**
         * A lazy reader is provided to allow fast skipping of bulk of work where tag or timestamp are to be used as
         * a basis for filtering the {@link EncodableHistogram} anyway. The reader is to be called only once.
         *
         * @param tag        histogram tag or null if none exist
         * @param timestamp  logged timestamp
         * @param length     logged interval length
         * @param lazyReader to be called if the histogram needs to be deserialized, given the tag/timestamp etc.
         * @return false to keep processing, true to stop
         */
        boolean onHistogram(String tag, double timestamp, double length, EncodableHistogramSupplier lazyReader);

        /**
         * @param t an exception observed while processing the log
         * @return false to keep processing, true to stop
         */
        boolean onException(Throwable t);
    }

    private static class LazyHistogramReader implements EncodableHistogramSupplier
    {

        private final Scanner scanner;
        private boolean gotIt = true;

        private LazyHistogramReader(Scanner scanner)
        {
            this.scanner = scanner;
        }

        private void allowGet()
        {
            gotIt = false;
        }

        @Override
        public EncodableHistogram read() throws DataFormatException
        {
            // prevent double calls to this method
            if (gotIt)
            {
                throw new IllegalStateException();
            }
            gotIt = true;

            final String compressedPayloadString = scanner.next();
            final ByteBuffer buffer = ByteBuffer.wrap(Base64.getDecoder().decode(compressedPayloadString));

            EncodableHistogram histogram = decodeFromCompressedByteBuffer(buffer, 0);

            return histogram;
        }
    }

    static EncodableHistogram decodeFromCompressedByteBuffer(ByteBuffer buffer, long minBarForHighestTrackableValue)
        throws DataFormatException
    {
        int cookie = buffer.getInt(buffer.position());
        return (EncodableHistogram) (isDoubleHistogramCookie(cookie) ?
            DoubleHistogram.decodeFromCompressedByteBuffer(buffer, minBarForHighestTrackableValue) :
            Histogram
                .decodeFromCompressedByteBuffer(buffer, minBarForHighestTrackableValue));
    }

    static boolean isDoubleHistogramCookie(int cookie)
    {
        return isCompressedDoubleHistogramCookie(cookie) || isNonCompressedDoubleHistogramCookie(cookie);
    }

    private static boolean isCompressedDoubleHistogramCookie(int cookie)
    {
        return cookie == 208802383;
    }

    private static boolean isNonCompressedDoubleHistogramCookie(int cookie)
    {
        return cookie == 208802382;
    }

    private final LazyHistogramReader lazyReader;
    protected final Scanner scanner;

    /**
     * @param inputFileName The name of the file to read from
     * @throws FileNotFoundException when unable to find inputFileName
     */
    public HistogramLogScanner(final String inputFileName) throws FileNotFoundException
    {
        this(new Scanner(new File(inputFileName)));
    }

    /**
     * @param inputStream The InputStream to read from
     */
    public HistogramLogScanner(final InputStream inputStream)
    {
        this(new Scanner(inputStream));
    }

    /**
     * @param inputFile The File to read from
     * @throws FileNotFoundException when unable to find inputFile
     */
    public HistogramLogScanner(final File inputFile) throws FileNotFoundException
    {
        this(new Scanner(inputFile));
    }

    private HistogramLogScanner(Scanner scanner)
    {
        this.scanner = scanner;
        this.lazyReader = new LazyHistogramReader(scanner);
        initScanner();
    }

    private void initScanner()
    {
        scanner.useLocale(Locale.US);
        scanner.useDelimiter("[ ,\\r\\n]");
    }

    /**
     * Close underlying scanner. Note that if initialized with InputStream then the stream is closed as a result.
     */
    @Override
    public void close()
    {
        scanner.close();
    }

    /**
     * Reads the log, delivering events to the provided handler until the handler signals to stop or the end of the log.
     * 
     * @param handler to handle s**t
     */
    public void process(EventHandler handler)
    {
        while (scanner.hasNextLine())
        {
            try
            {
                if (scanner.hasNext("\\#.*"))
                {
                    // comment line.
                    // Look for explicit start time or base time notes in comments:
                    if (scanner.hasNext("#\\[StartTime:"))
                    {
                        scanner.next("#\\[StartTime:");
                        if (scanner.hasNextDouble())
                        {
                            double startTimeSec = scanner.nextDouble(); // start time represented as seconds since epoch
                            if (handler.onStartTime(startTimeSec))
                            {
                                return;
                            }
                        }
                    }
                    else if (scanner.hasNext("#\\[BaseTime:"))
                    {
                        scanner.next("#\\[BaseTime:");
                        if (scanner.hasNextDouble())
                        {
                            double baseTimeSec = scanner.nextDouble(); // base time represented as seconds since epoch
                            if (handler.onBaseTime(baseTimeSec))
                            {
                                return;
                            }
                        }
                    }
                    else if (handler.onComment(scanner.next("\\#.*")))
                    {
                        return;
                    }
                    continue;
                }

                if (scanner.hasNext("\"StartTimestamp\".*"))
                {
                    // Legend line
                    continue;
                }

                String tagString = null;
                if (scanner.hasNext("Tag\\=.*"))
                {
                    tagString = scanner.next("Tag\\=.*").substring(4);
                }

                // Decode: startTimestamp, intervalLength, maxTime, histogramPayload
                final double logTimeStampInSec = scanner.nextDouble(); // Timestamp is expected to be in seconds
                final double intervalLengthSec = scanner.nextDouble(); // Timestamp length is expect to be in seconds
                scanner.nextDouble(); // Skip maxTime field, as max time can be deduced from the histogram.

                lazyReader.allowGet();
                if (handler.onHistogram(tagString, logTimeStampInSec, intervalLengthSec, lazyReader))
                {
                    return;
                }

            }
            catch (Throwable ex)
            {
                if (handler.onException(ex))
                {
                    return;
                }
            }
            finally
            {
                if (scanner.hasNextLine())
                {
                    scanner.nextLine(); // Move to next line.
                }
            }
        }
        return;
    }

    /**
     * Indicates whether or not additional intervals may exist in the log
     *
     * @return true if additional intervals may exist in the log
     */
    public boolean hasNextLine()
    {
        return scanner.hasNextLine();
    }
}
