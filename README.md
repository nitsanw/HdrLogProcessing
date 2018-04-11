# HdrLogProcessing

Utilities for HDR Histogram logs manipulation. This repo currently includes utilities for summarizing and unioning of logs.

 - Requires Maven to build and JDK8 to build/run.
 - Released under BSD licence.

For brevity in the following examples, lets assume you built the project and added the following alias:

    alias hodor=java -cp HdrLogProcessing-1.0-SNAPSHOT-jar-with-dependencies.jar

## Summary tool
Using the above alias run:

    $ hodor SummarizeHistogramLogs [...]

SummarizeHistogramLogsRange supports the following options:

    -start (-s) N                          : relative log start time in seconds, (default: 0.0)
    -end (-e) N                            : relative log end time in seconds, (default: MAX_DOUBLE)
    -ignoreTag (-it)                       : summary should not be split by tag, (default: false)
    -inputFile (-if) VAL                   : add an input hdr log from input path, also takes regexp
    -inputFilePath (-ifp) VAL              : add an input file by path relative to working dir or absolute
    -inputPath (-ip) VAL                   : set path to use for input files, defaults to current folder
    -outputBucketSize (-obs) N             : csv output bucket size, (default: 100)
    -outputFile (-of) VAL                  : set an output file destination, default goes to sysout
    -outputValueUnitRatio (-ovr) N         : output value unit ratio, (default: 1.0)
    -percentilesOutputTicksPerHalf (-tph) N: ticks per half percentile, used for hgrm output, (default: 5)
    -summaryType (-st) [CSV | PERCENTILES | HGRM]:  : summary type: csv, percentiles, hgrm                                   
    -verbose (-v) : verbose logging, (default: false)

This is useful when for example you are face with a histogram log you have collected from your application over time and you wish to summarize the percentiles from the full run:

     $ hodor SummarizeHistogramLogsRange -if my-awesome-app-latencies.hdr
     TotalCount=27663673
     Period(ms)=205823
     Throughput(ops/sec)=134405.16
     Min=263
     Mean=6561.99
     50.000ptile=5491
     90.000ptile=8887
     99.000ptile=49023
     99.900ptile=72767
     99.990ptile=92927
     99.999ptile=116415
     Max=145151

Now perhaps the first 200 seconds of this run are an unstable warmup period I wish to exclude from my summary:

    $ hodor SummarizeHistogramLogs -if my-awesome-app-latencies.hdr -s 200

Or maybe I got several logs, from several runs and I want an overall summary, excluding the first 60 seconds of the run and saving the output into a file:

    $ hodor SummarizeHistogramLogs -if run1.hdr -if run2.hdr -if run3.hdr -s 60 -of runs-summary.out

    -OR you could use a regexp to get all the files-

    $ hodor SummarizeHistogramLogs -if ^run.*.hdr -s 60 -of runs-summary.out

The default output is percentiles as shown above. We support HGRM output if you wish to plot the result with the useful plotter in HdrHistogram, and a CSV format to enable statistical analysis with other tools. The HGRM output with an output file will result in a file per tagged summary with the convention of: _outputfile.tag.hgrm_

The summary tool supports tags, and if your logs contains histograms of different tags they will get summarized separately. You can use the '-it|ignoreTag' option to summarize all tags together.

## Union tool
Using the above alias run:

    $ hodor UnionHistogramLogs [...]

UnionHistogramLogs supports the following options:

    -end (-e) N                 : relative log end time in seconds, (default: MAX_DOUBLE)
    -inputFile (-if) VAL        : add an input hdr log from input path, also takes regexp
    -inputPath (-ip) VAL        : set path to use for input files, defaults to current folder
    -outputFile (-of) VAL       : set an output file destination, default goes to sysout
    -relative (-r)              : relative timeline merge, (default: true)
    -start (-s) N               : relative log start time in seconds, (default: 0.0)
    -taggedInputFile (-tif) VAL : a <tag>=<filename> add an input file, tag all
                                  histograms from this file with tag. If histograms
                                  have a tag it will be conactanated to file tag
                                  <file-tag>::<histogram-tag>.
    -verbose (-v)               : verbose logging, (default: false)

Sometimes you got lots of files, and you really wish you could just throw them all into one file. For example, lets say you used 3 separate load-generating clients to measure your server latencies. You can union all the logs into a single log as follows:

    $ hodor UnionHistogramLogs -if ^load-gen.*.hdr -of union-load-gens.hdr

The above union will use absolute time so the result will be as if all load generators were logged from a single source (assuming the clocks are reasonablely in sync). You may want to collect multiple runs timelines into a single union. This is possible using the '-r' option.

If each load generator represents a different operation you could use tags to differentiate them in the union:

    $ hodor UnionHistogramLogs -tif READ=load-gen1.hdr -tif READ=load-gen2.hdr -tif WRITE=load-gen3.hdr -of union-load-gens.hdr

## Split tool
Using the above alias run:

    $ hodor SplitHistogramLogs [...]

SplitHistogramLogs supports the following options:

    -end (-e) N          : relative log end time in  seconds,    (default: MAX_DOUBLE)
    -excludeTag (-et) VAL : add a tag to exclude from input, 'default' is a special tag for the null tag.
    -includeTag (-it) VAL : when include tags are used only the explicitly included will be split out, 'default' is a special tag for the null tag.
    -inputFile (-if) VAL : set the input hdr log from input     path
    -inputPath (-ip) VAL : set path to use for input files, defaults to current folder
    -start (-s) N        : relative log start time in  seconds,    (default: 0.0)
    -verbose (-v)        : verbose logging, (default:  false)

Some tools do not support tags yet, so you may want to split a log into several logs for post processing.

    $ hodor SplitHistogramLogs -if taggyLog.hdr

Will result in the creation of a log file per tag, with the default tag going to the 'default' file. So tags A,B,C will end up in the files A.taggyLog.hdr, B.taggyLog.hdr, C.taggyLog.hdr respectively.
If you need only certain tags A,B:

    $ hodor SplitHistogramLogs -if taggyLog.hdr -it A -it B

## HDR to CSV tool

Using the above alias, run:

    $ hodor HdrToCsv -i INPUT_FILE

It will result in a strict transformation of a log file to CSV.  Intervals will
be preserved.  For each interval, important percentiles will be written in
dedicated columns.  The resulting CSV is printed on stdout.

Example usage:

```
$ hodor HdrToCsv -i input.hgrm | tee output.csv
#Timestamp,Throughput,Min,Avg,p50,p90,p95,p99,p999,p9999,Max
1523292112.000,2364,60608,143515,113599,221823,268543,442367,1638399,6348799,6348799
1523292113.000,192,64672,130923,116287,188031,205823,260351,366591,366591,366591
1523292114.000,384,67520,144460,118527,213759,264703,414463,1793023,1793023,1793023
...
```
