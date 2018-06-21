package psy.lob.saw;

import org.HdrHistogram.Histogram;

public class HistogramIterator implements Comparable<HistogramIterator>
{
    private final OrderedHistogramLogReader reader;
    private Histogram next;
    private final String tag;
    private final boolean relative;

    public HistogramIterator(OrderedHistogramLogReader reader, boolean relative)
    {
        this(reader, null, relative);
        read();
    }

    public HistogramIterator(OrderedHistogramLogReader reader, String tag, boolean relative)
    {
        this.reader = reader;
        this.tag = tag;
        this.relative = relative;
        // init the reader
        read();
    }

    private void read()
    {
        do
        {
            next = (Histogram) reader.nextIntervalHistogram();
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

    public Histogram next()
    {
        Histogram c = next;
        read();
        return c;
    }

    public boolean hasNext()
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

    public double getStartTimeSec()
    {
        return reader.getStartTimeSec();
    }
}
