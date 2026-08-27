using Kafka.Tool.Cli.Message;
using McMaster.Extensions.CommandLineUtils;

namespace Kafka.Tool.Cli.Topic
{
    [Command("consumer", Description = "Manage consumers"),
        Subcommand(typeof(ConsumeOffsetCommand)), Subcommand(typeof(ConsumeGroupsCommand))]
    public class ConsumeCommand
    {
        private int OnExecute(IConsole console)
        {
            console.Error.WriteLine("You must specify an action. See --help for more details.");
            return 1;
        }
    }
}
