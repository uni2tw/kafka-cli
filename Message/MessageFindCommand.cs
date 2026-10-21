using System;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Kafka.Tool.Cli.Kafka;
using System.Text;

namespace Kafka.Tool.Cli.Message
{
    [Command("find", Description = "dump a message to console or file",
        AllowArgumentSeparator = true)]
    public class MessageFindCommand
    {

        [Required(ErrorMessage = "You must specify the topic")]
        [Option("-t|--topic", Description = "Source topic name")]
        public string TopicName { get; }

        [Option("-so", CommandOptionType.NoValue, Description = "show comment")]
        public bool showComment { get; }

        [Option("-n|--ntop", Description = "number of limit")]
        public int? Top { get; }
        
        [Required(ErrorMessage = "You must specify the keyword, use * scan all")]
        [Argument(0, Description = "Keyword to find")]
        public string Keyword { get; }

        [Option("-s|--start", Description = "Start time")]
        public string StartTime { get; }

        [Option("-e|--end", Description = "End time")]
        public string EndTime { get; }

        [Option("-d|--debug", CommandOptionType.NoValue, Description = "Debug mode")]
        public bool Debug { get; }

		[Option("-b|--beta", CommandOptionType.NoValue, Description = "Beta test")]
		public bool Beta { get; }

		[Option("-p|--path", Description = "Specify Output Path")]
		public string Path { get; }

		private async Task<int> OnExecute(IConsole console)
        {
            try
            {
                DateTime? parsedStartTime;
                DateTime? parsedEndTime;
                Helper.TryParseNullableDateTime(StartTime, out parsedStartTime);
                Helper.TryParseNullableDateTime(EndTime, out parsedEndTime);

                if (Debug)
                {
                    Console.WriteLine("接收到的: " + Keyword);
                    Console.WriteLine("字節資料: " + BitConverter.ToString(Encoding.UTF8.GetBytes(Keyword)));
                    byte[] bytes = Encoding.UTF8.GetBytes(Keyword);
                    string decodedString = Encoding.UTF8.GetString(bytes);
                    Console.WriteLine($"解碼字串: {decodedString}");
                }

				//await KafkaClient.FindMessage2Async(TopicName, Keyword, parsedStartTime, parsedEndTime,
				//	showComment, Top.GetValueOrDefault(100), Debug, Path);
				if (Beta)
				{
					await KafkaClient.FindMessage2Async(TopicName, Keyword, parsedStartTime, parsedEndTime,
						showComment, Top.GetValueOrDefault(100), Debug, Path);
				}
				else
				{
					await KafkaClient.FindMessageAsync(TopicName, Keyword, parsedStartTime, parsedEndTime,
						showComment, Top.GetValueOrDefault(100), Debug, Path);
				}
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