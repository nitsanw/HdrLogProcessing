package psy.lob.saw;

import org.HdrHistogram.Histogram;

import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UnionHistograms implements Runnable
{

    private static class UnionHistogram
    {
        final Histogram h;
        int index;

        private UnionHistogram(int numberOfSignificantValueDigits)
        {
            this.h = new Histogram(numberOfSignificantValueDigits);
        }
    }
    private final boolean verbose;
    private final PrintStream verboseOut;
    private final List<HistogramIterator> inputs;
    private final HistogramSink output;
    private final long targetUnionMs;

    public UnionHistograms(
        boolean verbose,
        PrintStream verboseOut,
        List<HistogramIterator> inputs,
        HistogramSink output)
    {
        this(verbose,verboseOut, inputs, output, 0);
    }
    
    public UnionHistograms(
        boolean verbose,
        PrintStream verboseOut,
        List<HistogramIterator> inputs,
        HistogramSink output,
        long targetUnionMs)
    {
        this.verbose = verbose;
        this.verboseOut = verboseOut;
        this.inputs = inputs;
        this.output = output;
        this.targetUnionMs = targetUnionMs;
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

        Map<String, UnionHistogram> unionedByTag = new HashMap<>();
        while (!ins.isEmpty())
        {
            HistogramIterator input = ins.get(0);
            Histogram next = input.next();

            UnionHistogram union = unionedByTag.computeIfAbsent(next.getTag(), k ->
            {
                UnionHistogram u = new UnionHistogram(next.getNumberOfSignificantValueDigits());
                u.h.setEndTimeStamp(0L);
                u.h.setStartTimeStamp(Long.MAX_VALUE);
                u.h.setTag(k);
                return u;
            });
            Histogram unionHgrm = union.h;
            final int unionIndex = union.index;

            long nextStart = next.getStartTimeStamp();
            long nextEnd = next.getEndTimeStamp();

            long unionStart = unionHgrm.getStartTimeStamp();
            long unionEnd = unionHgrm.getEndTimeStamp();
            // iterators are sorted, so we know nextStart >= unionStart
            boolean rollover = false;

            // new union
            if (unionStart == Long.MAX_VALUE)
            {
                addNext(input.source(), unionIndex, next, unionHgrm);
                // expand union length to allow more intervals to fall into the same union
                if (unionHgrm.getEndTimeStamp() - unionHgrm.getStartTimeStamp() < targetUnionMs)
                {
                    unionHgrm.setEndTimeStamp(unionHgrm.getStartTimeStamp()  + targetUnionMs);
                }
            }
            // next interval is inside union interval
            else if (nextStart < unionEnd && nextEnd <= unionEnd)
            {
                addNext(input.source(), unionIndex, next, unionHgrm);
            }
            // next interval starts before the end of this interval, but is not contained by it
            else if (nextStart < unionEnd)
            {
                double nextIntervalLength = nextEnd - nextStart;
                double overlap = (unionEnd - nextStart) / nextIntervalLength;
                // 80% or more of next is in fact in the current union 
                if (overlap > 0.8)
                {
                    addNext(input.source(), unionIndex, next, unionHgrm);
                    // prevent an ever expanding union
                    unionHgrm.setStartTimeStamp(unionStart);
                    unionHgrm.setEndTimeStamp(unionEnd);
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
                outputUnion(unionIndex, unionHgrm);
                final int unionIndexNext = ++union.index;
                unionHgrm.reset();
                unionHgrm.setEndTimeStamp(0L);
                unionHgrm.setStartTimeStamp(Long.MAX_VALUE);
                unionHgrm.setTag(next.getTag());
                
                addNext(input.source(), unionIndexNext, next, unionHgrm);
                // expand union length to allow more intervals to fall into the same union
                if (unionHgrm.getEndTimeStamp() - unionHgrm.getStartTimeStamp() < targetUnionMs)
                {
                    unionHgrm.setEndTimeStamp(unionHgrm.getStartTimeStamp()  + targetUnionMs);
                }
            }
            // trim and sort
            ins.removeIf(e -> !e.hasNext());
            Collections.sort(ins);
        }
        // write last hgrms
        for (UnionHistogram u : unionedByTag.values())
        {
            outputUnion(u.index, u.h);
        }
    }

    private void outputUnion(int i, Histogram union)
    {
        if (verbose)
        {
            verboseOut.print("union, ");
            HdrHistogramUtil.logHistogramForVerbose(verboseOut, union, i);
        }
        output.accept(union);
    }

    private void addNext(String source, int i, Histogram next, Histogram union)
    {
        union.add(next);
        if (verbose)
        {
            verboseOut.print(source);
            verboseOut.print(", ");
            HdrHistogramUtil.logHistogramForVerbose(verboseOut, next, i);
        }
    }

}
