using System;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Kafka.Tool.Cli.Kafka;
using System.Net.Http;
using System.Text.Json;
using System.Collections.Generic;

namespace Kafka.Tool.Cli.Message
{
	[Command("remote-copy", Description = "Copy a message to the Kafka topic",
		AllowArgumentSeparator = true)]
	public class MessageRemoteCopyCommand
	{
		[Required(ErrorMessage = "You must specify the topic")]
		[Option("-t|--topic", Description = "Topic name")]
		public string TopicName { get; }

		[Required(ErrorMessage = "You must specify the source host")]
		[Option("-sh|--source-host", Description = "Topic name")]
		public string SourceHost { get; set; }


		[Required(ErrorMessage = "You must specify the source host")]
		[Option("-th|--target-host", Description = "Topic name")]

		public string TargetHost { get; set; }

		[Option("-so|--start-offset", Description = "Start offset")]
		public int? StartOffset { get; set; }
		[Option("-eo|--end-offset", Description = "End offset")]
		public int? EndOffset { get; set; }


		private async Task<int> OnExecute(IConsole console)
		{
			try
			{
				await KafkaClient.RemoteCopyMessageAsync(TopicName, SourceHost, TargetHost, StartOffset, EndOffset);
				return 0;
			}
			catch (JsonException ex)
			{
				console.WriteLine("µo¥Í¿ù»~, " + ex.Message);
				return await Task.FromResult(1);
			}
			catch (Exception e)
			{
				console.WriteLine($"Delivery failed: {e.Message}");
				return await Task.FromResult(1);
			}
		}
	}
}
