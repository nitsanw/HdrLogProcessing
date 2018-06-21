import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

class ParseAndRunUtil
{
    static void parseParamsAndRun(String[] args, Runnable app)
    {
        CmdLineParser parser = new CmdLineParser(app);
        try
        {
            parser.parseArgument(args);
            app.run();
        }
        catch (CmdLineException e)
        {
            System.out.println(e.getMessage());
            parser.printUsage(System.out);
        }
    }
}
