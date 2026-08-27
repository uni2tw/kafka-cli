using McMaster.Extensions.CommandLineUtils;
using Kafka.Tool.Cli.Config;
using Kafka.Tool.Cli.Message;
using Kafka.Tool.Cli.Topic;
using Kafka.Tool.Cli.Shell;

namespace Kafka.Tool.Cli.Kafka
{
    [Command(Name = "kafka-cli", Description = "Kafka Command Line Tool (kafka-cli) allows you to manage topics and produce/consume messages to/from Kafka cluster."),
     Subcommand(typeof(TopicCommand)), 
        Subcommand(typeof(MessageCommand), typeof(ConfigCommand), typeof(ConsumeCommand), typeof(ShellCommand))]
    public class KafkaCommand
    {
        private int OnExecute(CommandLineApplication app, IConsole console)
        {            
            app.ShowHelp();
            return 1;
        }                
    }
}
