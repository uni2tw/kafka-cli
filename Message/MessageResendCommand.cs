using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Kafka.Tool.Cli.Kafka;

namespace Kafka.Tool.Cli.Message
{

    [Command("resend", Description = "Re-publish past events of a topic to the tail of the same topic (every consumer group of the topic will receive them again)",
            AllowArgumentSeparator = true)]
    public class MessageResendCommand
    {

        [Required(ErrorMessage = "You must specify the topic")]
        [Option("-t|--topic", Description = "Topic name")]
        public string TopicName { get; }

        [Required(ErrorMessage = "You must specify the offset")]
        [Option("-o|--offset", Description = "Offsets to resend: '2', '2,3', '2-3' or a mix such as '2,5-7'")]
        public string Offset { get; }

        [Option("-p|--partition", Description = "Partition number. Required when the same offset exists in multiple partitions.")]
        public int? Partition { get; }

        [Option("-y|--yes", CommandOptionType.NoValue, Description = "Skip the confirmation prompt")]
        public bool Yes { get; }

        private async Task<int> OnExecute(IConsole console)
        {
            try
            {
                var offsets = KafkaClient.ParseOffsetList(Offset);

                // read everything first so a bad offset fails before anything is sent
                var messages = offsets
                    .Select(offset => KafkaClient.GetMessage(TopicName, offset, Partition))
                    .ToList();

                var groups = (await KafkaClient.GetConsumeGroupsAsync(TopicName)).ToList();
                KafkaClient.SetCachedConsumeGroups(TopicName, groups);

                WriteWarning(console, offsets, groups);

                if (!Yes && !Prompt.GetYesNo("確定要繼續嗎？", false))
                {
                    console.WriteLine("已取消，沒有發送任何事件。");
                    return 1;
                }

                var result = await KafkaClient.ProduceMessageAsync(TopicName, messages);
                console.WriteLine($"已補發 offset {FormatOffsets(offsets)} of {TopicName}: {result}");
                return 0;
            }
            catch (Exception e)
            {
                console.WriteLine($"{e.Message}");
                return 1;
            }
        }

        private void WriteWarning(IConsole console, List<int> offsets, List<string> groups)
        {
            console.WriteLine($"即將在 topic '{TopicName}' 重新發送 {offsets.Count} 筆事件（offset: {FormatOffsets(offsets)}）。");
            console.WriteLine("警告：重發的事件會寫入 topic 尾端，所有訂閱此 topic 的 consumer group 都會收到並重新處理，不只是漏掉事件的那一個。");
            if (groups.Count == 0)
            {
                console.WriteLine("目前查不到有 consumer group 訂閱此 topic。");
            }
            else
            {
                console.WriteLine($"目前訂閱此 topic 的 consumer group（{groups.Count} 個）：");
                foreach (var group in groups)
                {
                    console.WriteLine($"  - {group}");
                }
            }
        }

        private static string FormatOffsets(List<int> offsets)
        {
            const int maxShown = 20;
            var shown = string.Join(", ", offsets.Take(maxShown));
            return offsets.Count > maxShown ? shown + ", ..." : shown;
        }
    }
}
