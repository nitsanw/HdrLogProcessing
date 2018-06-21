package psy.lob.saw;

import org.HdrHistogram.Histogram;

public interface HistogramSink
{
    void startTime(double st);

    void accept(Histogram h);
}
