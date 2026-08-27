using System;
using System.ComponentModel.DataAnnotations;
using System.Threading;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Kafka.Tool.Cli.Kafka;
using System.Net.Http;
using System.Text.Json;
using System.Collections.Generic;

namespace Kafka.Tool.Cli.Message
{

    [Command("produce", Description = "Produce a message to the Kafka topic",
            AllowArgumentSeparator = true)]
    public class MessageProduceCommand
    {
        [Required(ErrorMessage = "You must specify the message or filename or url")]
        [Argument(0, Description = "Text message or file or url to produce")]
        public string Message { get; }

        [Required(ErrorMessage = "You must specify the topic")]
        [Option("-t|--topic", Description = "Topic name")]
        public string TopicName { get; }

        private async Task<int> OnExecute(IConsole console)
        {
            try
            {
                string message;
                Uri uri;
                string title = string.Empty;
                HttpClient client = new HttpClient();
                if (Uri.TryCreate(Message, UriKind.Absolute, out uri))
                {
                    title = uri.Segments[uri.Segments.Length - 1];
                    try
                    {
                        message = client.GetAsync(uri).Result.Content.ReadAsStringAsync().Result;
                    }
                    catch
                    {
                        Console.WriteLine($"{title} was not found");
                        return 0;
                    }                    
                }
                else if (System.IO.File.Exists(Message))
                {
                    message = System.IO.File.ReadAllText(Message);
                    title = new System.IO.FileInfo(Message).Name;
                }
                else
                {
                    message = Message;
                }

				//檢查格式是否正確
				List<string> messageList = new();
				try
				{
					using JsonDocument doc = JsonDocument.Parse(message);
					JsonElement root = doc.RootElement;					
					if (root.ValueKind == JsonValueKind.Array)
					{
						int count = root.GetArrayLength();
						Console.WriteLine($"這是一個 JSON 陣列，共有 {count} 筆資料。是否繼續？(Y/N)");

						string input = Console.ReadLine()?.Trim().ToUpper();
						if (input?.ToUpper() == "Y")
						{
							// 取得每個物件的序列化字串
							foreach (var item in root.EnumerateArray())
							{
								messageList.Add(item.GetRawText());
							}
						}
						else
						{
							Console.WriteLine("已中止。");
							return -1;
						}
					}
					else if (root.ValueKind == JsonValueKind.Object)
					{
						messageList.Add(message);
					}
					else
					{
						Console.WriteLine($"這是 JSON，但不是物件或陣列（型別為：{root.ValueKind}）");
					}
				}
				catch (JsonException)
				{
					Console.WriteLine("這不是合法的 JSON 格式。");
					return -1;
				}

                var topicInfo = await KafkaClient.ProduceMessageAsync(TopicName, messageList);

                console.WriteLine($"message {title} delivered to {topicInfo}");
                
                return await Task.FromResult(0);
            }
            catch (JsonException ex)
            {
                console.WriteLine("發生錯誤, " + ex.Message);
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