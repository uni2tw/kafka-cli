using System;
using System.ComponentModel.DataAnnotations;
using System.Threading;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Kafka.Tool.Cli.Config;
using Kafka.Tool.Cli.Kafka;

namespace Kafka.Tool.Cli.Message
{

    [Command("consume", Description = "Consume a message",
            AllowArgumentSeparator = true)]
    public class MessageConsumeCommand
    {
        [Required(ErrorMessage = "You must specify the topic")]
        [Option("-t|--topic", Description = "Topic name")]
        public string TopicName { get; }

        [Option("-g|--group", Description = "Group ID")]
        public string GroupId { get; } = "kafka-cli";

        [Option("-c|--commit", Description = "Commit the message. This prevents from receiving the same message twice for the one Group ID.")]
        public bool Commit { get; } = false;

        [Option("-p|--pause", Description = "Pause between producing messages in milliseconds")]
        public int Pause { get; } = 0;

        private async Task<int> OnExecute(IConsole console)
        {
            var config = ConfigService.Get();

            console.WriteLine($"Waiting for messages in {TopicName}:");

            KafkaClient.ConsumeMessage(TopicName, GroupId, Commit, (string topicInfo, string message) =>
            {
                console.WriteLine($"[{DateTime.Now.ToString("HH:mm:ss.ff")}] at {topicInfo}: {message}");
                if (Pause > 0)
                {
                    Thread.Sleep(Pause);
                }
            },
            (string error) =>
            {
                console.WriteLine($"Error occured: {error}");
            });

            return await Task.FromResult(0);
        }
    }
}