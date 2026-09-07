using System;
using System.ComponentModel.DataAnnotations;
using System.Threading;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Kafka.Tool.Cli.Kafka;

namespace Kafka.Tool.Cli.Message
{

    [Command("clone", Description = "clone a message",
            AllowArgumentSeparator = true)]
    public class MessageCloneCommand
    {        

        [Required(ErrorMessage = "You must specify the topic")]
        [Option("-t|--topic", Description = "Source topic name")]
        public string FromTopicName { get; }

        [Required(ErrorMessage = "You must specify the offset")]
        [Option("-o|--offset", Description = "Offset of messages at the topic will be cloned")]
        public string FromOffset { get; }
        
        [Option("-tt|--to-topic", Description = "Target topic name")]
        public string ToTopic { get; }

        [Option("-p|--partition", Description = "Partition number. Required when the same offset exists in multiple partitions.")]
        public int? Partition { get; }

        private async Task<int> OnExecute(IConsole console)
        {
            try
            {
                string toTopic = ToTopic ?? FromTopicName;
                var topicInfo = await KafkaClient.ProduceCloneMessageAsync(FromTopicName, FromOffset, toTopic, Partition);
                console.WriteLine($"message {FromOffset}@{FromTopicName} delivered to {topicInfo}@{toTopic}");
                return await Task.FromResult(0);
            }
            catch (Exception e)
            {
                console.WriteLine($"{e.Message}");
                return await Task.FromResult(1);
            }
        }
    }
}
