using System;
using System.ComponentModel.DataAnnotations;
using System.Threading;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Kafka.Tool.Cli.Kafka;

namespace Kafka.Tool.Cli.Message
{

    [Command("get", Description = "get a message by offset",
            AllowArgumentSeparator = true)]
    public class MessageGetCommand
    {

        [Required(ErrorMessage = "You must specify the topic")]
        [Option("-t|--topic", Description = "Source topic name")]
        public string TopicName { get; }

        [Argument(0, Description = "Offset or offset range ('1200-1210'). A negative index (e.g. '-1' for the newest message) cannot be passed here because it looks like an option; use -o|--offset instead.")]
        public string Offset { get; }

        [Option("-o|--offset", Description = "Same as the positional offset argument, but also accepts a negative index ('-1' = newest message, '-100' = 100th from the newest). Required when the value starts with '-'.")]
        public string OffsetOption { get; }

        [Option("-p|--partition", Description = "Partition number. Required when the same offset exists in multiple partitions.")]
        public int? Partition { get; }

        private async Task<int> OnExecute(IConsole console)
        {
            try
            {
                var offset = !string.IsNullOrWhiteSpace(OffsetOption) ? OffsetOption : Offset;
                if (string.IsNullOrWhiteSpace(offset))
                {
                    console.WriteLine("You must specify the offset, e.g. 'get 12' or 'get -o -1' for the newest message.");
                    return await Task.FromResult(1);
                }

                await KafkaClient.DumpMessageAsync(TopicName, offset, Partition);
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
