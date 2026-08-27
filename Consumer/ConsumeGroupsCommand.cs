using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Kafka.Tool.Cli.Kafka;

namespace Kafka.Tool.Cli.Topic
{

    [Command("groups", Description = "Consume Get Groups"), HelpOption]
    public class ConsumeGroupsCommand
    {
        [Option("-t|--topic", Description = "Topic name")]
        public string TopicName { get; }

        private async Task<int> OnExecute(IConsole console)
        {
            try
            {
                var groups = string.IsNullOrWhiteSpace(TopicName)
                    ? KafkaClient.GetConsumeGroups()
                    : await KafkaClient.GetConsumeGroupsAsync(TopicName);

                if (!string.IsNullOrWhiteSpace(TopicName))
                {
                    KafkaClient.SetCachedConsumeGroups(TopicName, groups.ToList());
                }

                foreach (var group in groups)
                {
                    console.WriteLine(group);
                }
                return await Task.FromResult(0);
            }
            catch (Exception e)
            {
                console.WriteLine($"An error occured get consume groups: {e.Message}");
                return await Task.FromResult(1);
            }
        }
    }
}
