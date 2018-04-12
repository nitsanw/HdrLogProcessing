import java.util.Arrays;
import java.util.stream.Stream;

/**
 * This class is the main entry point of HdrLogProcessing.  It consumes the
 * first CLI parameter entered by the user and tries to match it against known
 * {@link Command}s.  If a match is found, then the {@code main(String[] args}
 * method of the class responsible for said command is invoked.
 *
 * This class is mostly a shortcut so that the user does not need to remember
 * the class name to be invoked (like {@code UnionHistogramLogs}).
 */
public class CommandDispatcherMain
{
    private static void usage()
    {
        System.err.println("Usage: hodor COMMAND [options...]");
        System.err.println("");
        System.err.println("Valid commands:");
        for (Command command : Command.values())
        {
            System.err.println("  " + command.niceName());
        }
    }

    public static void main(String[] args) throws Exception
    {
        if (args.length < 1)
        {
            System.err.println("Error: missing command");
            usage();
        }
        else if (!Command.isValid(args[0]))
        {
            System.err.println("Error: invalid command '" + args[0] + "'");
            usage();
        }
        else
        {
            // Remove the command name from `args` so that the all the remaining
            // arguments can be passed to the underlying class.
            String[] withoutCommand = Arrays.copyOfRange(args, 1, args.length);
            Command.fromUserInput(args[0])
                    .mainClass
                    .getMethod("main", String[].class)
                    .invoke(null, (Object) withoutCommand);
        }
    }

    private enum Command
    {
        TO_CSV(HdrToCsv.class),
        SPLIT(SplitHistogramLogs.class),
        SUMMARIZE(SummarizeHistogramLogs.class),
        UNION(UnionHistogramLogs.class);

        private final Class<?> mainClass;

        Command(Class<?> mainClass)
        {
            this.mainClass = mainClass;
        }

        private static boolean isValid(String command)
        {
            return Stream.of(values())
                    .anyMatch(c -> c.niceName().equals(command));
        }

        private static Command fromUserInput(String command)
        {
            return Stream.of(values())
                    .filter(c -> c.niceName().equals(command))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Invalid command '" + command + "'"));
        }

        private String niceName()
        {
            return sanitize(name());
        }

        private static String sanitize(String s)
        {
            return s.replace("_", "-").toLowerCase();
        }
    }
}
