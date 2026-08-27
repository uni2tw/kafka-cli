using McMaster.Extensions.CommandLineUtils;

namespace Kafka.Tool.Cli.Message
{
    [Command("message", Description = "Produce and consume messages"),
        Subcommand(typeof(MessageProduceCommand)),
        Subcommand(typeof(MessageConsumeCommand)),
        Subcommand(typeof(MessageCloneCommand)),
        Subcommand(typeof(MessageGetCommand)),
		Subcommand(typeof(MessageRemoteCopyCommand)),
		Subcommand(typeof(MessageFindCommand)),
		Subcommand(typeof(MessageFindManyCommand)),
        Subcommand(typeof(MessageFindPathCommand)),
        ]
    public class MessageCommand
    {
        private int OnExecute(IConsole console)
        {
            console.Error.WriteLine("You must specify an action. See --help for more details.");
            return 1;
        }
    }
}
