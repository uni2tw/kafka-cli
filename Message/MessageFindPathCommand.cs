using System;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Kafka.Tool.Cli.Kafka;

namespace Kafka.Tool.Cli.Message
{
    [Command("find-path", Description = "dump a message to console or file",
        AllowArgumentSeparator = true)]
    public class MessageFindPathCommand
    {

        [Required(ErrorMessage = "You must specify the topic")]
        [Option("-t|--topic", Description = "Source topic name")]
        public string TopicName { get; }

        [Required(ErrorMessage = "You must specify the start offset, exg. 60849")]
        [Option("-so|--start-offset", Description = "start offset")]
        public int? startOffset { get; }


        [Option("-eo|--end-offset", Description = "end offset")]
        public int? endOffset { get; }

        [Option("-mr|--max-result", Description = "max result")]
        public int? maxResult { get; }

        [Option("-sc", CommandOptionType.NoValue, Description = "show comment")]
        public bool showComment { get; }

        [Option("-p|--partition", Description = "Partition number. Omit to scan all partitions.")]
        public int? Partition { get; }

        [Required(ErrorMessage = "You must specify the jsonPath, exg. /SalesMix/SaleCode")]
        [Argument(0, Description = "path to find")]
        public string JsonPath { get; }

        private async Task<int> OnExecute(IConsole console)
        {
            try
            {
                await KafkaClient.FindMessagesByJsonPathAsync(
                    TopicName, JsonPath, startOffset ?? 0, endOffset , maxResult ?? 1000, showComment, Partition);
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
