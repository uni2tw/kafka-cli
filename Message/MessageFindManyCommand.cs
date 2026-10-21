using System;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Kafka.Tool.Cli.Kafka;

namespace Kafka.Tool.Cli.Message
{
    [Command("find-many", Description = "dump a message to console or file",
        AllowArgumentSeparator = true)]
    public class MessageFindManyCommand
    {

        [Required(ErrorMessage = "You must specify the topic")]
        [Option("-t|--topic", Description = "Source topic name")]
        public string TopicName { get; }

        [Option("-n|--ntop", Description = "number of limit")]
        public int? Top { get; }
        
        [Required(ErrorMessage = "You must specify the keyword")]
        [Argument(0, Description = "Keyword to find")]
        public string Keyword { get; }

        private async Task<int> OnExecute(IConsole console)
        {
            try
            {
                string[] keywords = Keyword.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                await KafkaClient.FindManyMessageAsync(TopicName, keywords, Top.GetValueOrDefault(100));
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