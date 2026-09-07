using System;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Kafka.Tool.Cli.Kafka;

namespace Kafka.Tool.Cli.Topic
{
    [Command("offset", Description = "Consume Set Offset"), HelpOption]
    public class ConsumeOffsetCommand
    {
        [Required(ErrorMessage = "You must specify a topic name")]
        [Option("-t|--topic", Description = "Topic name")]
        public string TopicName { get; }

        [Required(ErrorMessage = "You must specify a group name")]
        [Option("-g|--group", Description = "Group name")]
        public string GroupName { get; }

        //[Required(ErrorMessage = "You must specify offset from current")]
        [Option("-ofc|--offsetFromCurrent", Description = "offset from current")]
        public long? OffsetFromCurrent { get; }

        //[Required(ErrorMessage = "You must specify offset")]
        [Option("-o|--offset", Description = "offset")]
        public long? Offset { get; }

        [Option("-p|--partition", Description = "Partition number. Omit to update all partitions.")]
        public int? Partition { get; }

        private async Task<int> OnExecute(IConsole console)
        {
            try
            {
                if (Offset != null)
                {
                    await KafkaClient.SetConsumerOffset(TopicName, GroupName, Offset.Value, false, Partition);
                }
                else if (OffsetFromCurrent != null)
                {
                    await KafkaClient.SetConsumerOffset(TopicName, GroupName, OffsetFromCurrent.Value, true, Partition);
                }
                else
                {
                    Console.WriteLine("You must specify offset -o or offset_from_current -ofc");
                }
                return await Task.FromResult(0);
            }
            catch (Exception e)
            {
                console.WriteLine($"An error occured set consumer offset '{GroupName}': {e.Message}");
                return await Task.FromResult(1);
            }
        }
    }    
}
