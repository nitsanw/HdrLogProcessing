package psy.lob.saw;

import org.HdrHistogram.Histogram;

import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UnionHistograms implements Runnable
{

    private final boolean verbose;
    private final PrintStream verboseOut;
    private final List<HistogramIterator> inputs;
    private final HistogramSink output;

    public UnionHistograms(
        boolean verbose,
        PrintStream verboseOut,
        List<HistogramIterator> inputs,
        HistogramSink output)
    {
        this.verbose = verbose;
        this.verboseOut = verboseOut;
        this.inputs = inputs;
        this.output = output;
    }

    @Override
    public void run()
    {
        List<HistogramIterator> ins = inputs;
        ins.removeIf(e -> !e.hasNext());
        Collections.sort(ins);

        if (ins.isEmpty())
        {
            if (verbose)
            {
                verboseOut.println("Input files do not contain range");
            }
            return;
        }

        output.startTime(ins.get(0).getStartTimeSec());

        Map<String, Histogram> unionedByTag = new HashMap<>();
        // iterators are now sorted by start time
        int i = 0;
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
                i = addNext(i, next, union);
            }
            else if (nextStart < unionEnd && nextEnd <= unionEnd)
            {
                i = addNext(i, next, union);
            }
            else if (nextStart < unionEnd)
            {
                double nLength = nextEnd - nextStart;
                double overlap = (unionEnd - nextStart) / nLength;
                if (overlap > 0.8)
                {
                    i = addNext(i, next, union);
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
                i = outputUnion(i, union);
                union.reset();
                union.setEndTimeStamp(0L);
                union.setStartTimeStamp(Long.MAX_VALUE);
                union.setTag(next.getTag());
                i = addNext(i, next, union);
            }
            // trim and sort
            ins.removeIf(e -> !e.hasNext());
            Collections.sort(ins);
        }
        // write last hgrms
        for (Histogram u : unionedByTag.values())
        {
            i = outputUnion(i, u);
        }
    }

    private int outputUnion(int i, Histogram union)
    {
        if (verbose)
        {
            verboseOut.print("union, ");
            HdrHistogramUtil.logHistogramForVerbose(verboseOut, union, i++);
        }
        output.accept(union);
        return i;
    }

    private int addNext(int i, Histogram next, Histogram union)
    {
        union.add(next);
        if (verbose)
        {
            verboseOut.print("input, ");
            HdrHistogramUtil.logHistogramForVerbose(verboseOut, next, i++);
        }
        return i;
    }

}
