using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Tool.Cli.Config;
using McMaster.Extensions.CommandLineUtils;
using static Confluent.Kafka.ConfigPropertyNames;
using static Microsoft.Extensions.Logging.EventSource.LoggingEventSource;

namespace Kafka.Tool.Cli.Kafka
{
    public static class KafkaClient
    {
        private static readonly ConcurrentDictionary<string, IReadOnlyList<string>> TopicGroupCache =
            new(StringComparer.OrdinalIgnoreCase);
        private static readonly ConcurrentDictionary<string, DateTime> TopicGroupCacheRefreshedAt =
            new(StringComparer.OrdinalIgnoreCase);
        private static readonly ConcurrentDictionary<string, byte> TopicGroupRefreshInProgress =
            new(StringComparer.OrdinalIgnoreCase);
        private static readonly TimeSpan TopicGroupCacheTtl = TimeSpan.FromSeconds(60);
        private static int TopicGroupCacheInitialized;

        private sealed class TopicMessageRecord
        {
            public required TopicPartitionOffset TopicPartitionOffset { get; init; }
            public required string Message { get; init; }
        }

        private static List<TopicPartition> GetTopicPartitions(IAdminClient adminClient, string topicName)
        {
            var config = ConfigService.Get();
            var metadata = adminClient.GetMetadata(TimeSpan.FromMilliseconds(config.TimeoutInMs));
            var topic = metadata.Topics.FirstOrDefault(x => x.Topic == topicName);
            if (topic == null)
            {
                throw new Exception($"topic {topicName} was not found.");
            }

            return topic.Partitions
                .Select(x => new TopicPartition(topicName, new Partition(x.PartitionId)))
                .OrderBy(x => x.Partition.Value)
                .ToList();
        }

        private static List<TopicPartition> GetTopicPartitions(string topicName, string? bootstrapServers = null)
        {
            var config = ConfigService.Get();
            using var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = bootstrapServers ?? config.BrokerHost,
                SocketTimeoutMs = config.TimeoutInMs
            }).Build();

            return GetTopicPartitions(adminClient, topicName);
        }

        public static IReadOnlyList<TopicPartition> GetTopicPartitions(string topicName)
        {
            return GetTopicPartitions(topicName, null);
        }

        private static List<TopicPartition> ResolveTopicPartitions(string topicName, int? partition, string? bootstrapServers = null)
        {
            var partitions = GetTopicPartitions(topicName, bootstrapServers);
            if (!partition.HasValue)
            {
                return partitions;
            }

            var match = partitions.FirstOrDefault(x => x.Partition.Value == partition.Value);
            if (match == default)
            {
                throw new Exception($"topic {topicName} partition {partition.Value} was not found.");
            }

            return new List<TopicPartition> { match };
        }

        private static List<TopicPartitionOffset> BuildAssignments(IEnumerable<TopicPartition> partitions, long? offset = null)
        {
            return partitions
                .Select(tp => new TopicPartitionOffset(tp, offset.HasValue ? new Offset(offset.Value) : Offset.Beginning))
                .ToList();
        }

        private static TopicMessageRecord? TryConsumeAssignedMessage(
            IConsumer<Ignore, string> consumer,
            TopicPartitionOffset assignment,
            int timeoutMs)
        {
            try
            {
                consumer.Assign(new[] { assignment });
                var result = consumer.Consume(TimeSpan.FromMilliseconds(timeoutMs));
                if (result == null || result.IsPartitionEOF || result.Offset != assignment.Offset)
                {
                    return null;
                }

                return new TopicMessageRecord
                {
                    TopicPartitionOffset = result.TopicPartitionOffset,
                    Message = Helper.UnicodeToString(result.Message.Value)
                };
            }
            catch (ConsumeException)
            {
                return null;
            }
        }

        private static List<TopicMessageRecord> GetMessagesByOffset(string topicName, int offset, int? partition = null)
        {
            var config = ConfigService.Get();
            var partitions = ResolveTopicPartitions(topicName, partition);
            var conf = new ConsumerConfig
            {
                BootstrapServers = config.BrokerHost,
                GroupId = "temp-" + Guid.NewGuid().ToString().ToLower(),
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var matches = new List<TopicMessageRecord>();
            using var consumer = new ConsumerBuilder<Ignore, string>(conf).Build();
            foreach (var assignment in BuildAssignments(partitions, offset))
            {
                var record = TryConsumeAssignedMessage(consumer, assignment, config.TimeoutInMs);
                if (record != null)
                {
                    matches.Add(record);
                }
            }

            try { consumer.Close(); } catch { }
            return matches;
        }

        private static TopicMessageRecord GetSingleMessageByOffset(string topicName, int offset, int? partition = null)
        {
            var matches = GetMessagesByOffset(topicName, offset, partition);
            if (matches.Count == 0)
            {
                throw new Exception($"Message not found, topic={topicName}, offset={offset}");
            }

            if (!partition.HasValue && matches.Count > 1)
            {
                throw new Exception($"Offset {offset} exists in multiple partitions. Specify --partition.");
            }

            return matches[0];
        }

        public static IEnumerable<string> ListTopics(string filter = "")
        {
            var config = ConfigService.Get();

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = config.BrokerHost,
                SocketTimeoutMs = config.TimeoutInMs
            })
            .Build())
            {
                try
                {
                    var metaData = adminClient.GetMetadata(TimeSpan.FromMilliseconds(config.TimeoutInMs));
                    var topics = metaData.Topics.Select(tm => tm.Topic);
                    if (!string.IsNullOrWhiteSpace(filter))
                    {
                        topics = topics.Where(t => t.Contains(filter, StringComparison.OrdinalIgnoreCase));
                    }
                    return topics.OrderBy(t => t);
                }
                catch (Exception e)
                {
                    throw new Exception($"{e.Message}");
                }
            }
        }

        public static async Task<Dictionary<TopicPartition, (long? StartOffset, long? EndOffset)>> GetTopicRangeOffsetAsync(
            string topicName,
            int? partition = null)
        {
            await Task.CompletedTask;

            var config = ConfigService.Get();
            var result = new Dictionary<TopicPartition, (long? StartOffset, long? EndOffset)>();
            var partitions = ResolveTopicPartitions(topicName, partition);

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = config.BrokerHost,
                GroupId = "temp-" + Guid.NewGuid().ToString().ToLower(),
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
            foreach (var tp in partitions)
            {
                var watermark = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromMilliseconds(config.TimeoutInMs));
                long? startOffset = watermark.Low.Value;
                long? endOffset = watermark.High.Value > watermark.Low.Value
                    ? watermark.High.Value - 1
                    : null;

                if (startOffset == Offset.Unset.Value)
                {
                    startOffset = null;
                }

                result[tp] = (startOffset, endOffset);
            }

            try { consumer.Close(); } catch { }
            return result;
        }

        public static async Task DeleteGroup(string groupName)
        {
            var config = ConfigService.Get();

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = config.BrokerHost,
                SocketTimeoutMs = config.TimeoutInMs
            })
            .Build())
            {
                try
                {
                    List<string> groupNames = new List<string>();
                    groupNames.Add(groupName);
                    await adminClient.DeleteGroupsAsync(groupNames);
                }
                catch (Exception e)
                {
                    throw new Exception($"{e.Message}");
                }
            }
        }

        public static IEnumerable<string> GetConsumeGroups()
        {
            var config = ConfigService.Get();

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = config.BrokerHost,
                SocketTimeoutMs = config.TimeoutInMs
            })
            .Build())
            {
                try
                {
                    var groups = adminClient.ListGroups(TimeSpan.FromMilliseconds(config.TimeoutInMs));
                    return groups.Select(x => x.Group).OrderBy(x => x);
                }
                catch (Exception e)
                {
                    throw new Exception($"{e.Message}");
                }
            }
        }

		//public static IEnumerable<string> GetConsumeGroups(string topicName)
		//{
		//    var config = ConfigService.Get();
		//    var partitions = GetTopicPartitions(topicName);
		//    if (partitions.Count == 0)
		//    {
		//        return Array.Empty<string>();
		//    }

		//    var groups = GetConsumeGroups().ToList();
		//    var matched = new List<string>();
		//    var topicPartitions = partitions.ToList();

		//    using var adminClient = new AdminClientBuilder(new AdminClientConfig
		//    {
		//        BootstrapServers = config.BrokerHost,
		//        SocketTimeoutMs = config.TimeoutInMs
		//    }).Build();

		//    foreach (var group in groups)
		//    {
		//        try
		//        {
		//            var result = adminClient.ListConsumerGroupOffsetsAsync(
		//                new[]
		//                {
		//                    new ConsumerGroupTopicPartitions(group, topicPartitions)
		//                },
		//                new ListConsumerGroupOffsetsOptions
		//                {
		//                    RequestTimeout = TimeSpan.FromMilliseconds(config.TimeoutInMs)
		//                })
		//                .GetAwaiter()
		//                .GetResult();

		//            var partitionsWithOffsets = result
		//                .SelectMany(x => x.Partitions)
		//                .Where(x => x.Topic == topicName && !x.Offset.IsSpecial && x.Offset != Offset.Unset);

		//            if (partitionsWithOffsets.Any())
		//            {
		//                matched.Add(group);
		//            }
		//        }
		//        catch
		//        {
		//        }
		//    }

		//    return matched.OrderBy(x => x);
		//}

		public static async Task<IEnumerable<string>> GetConsumeGroupsAsync(string topicName)
		{
			var config = ConfigService.Get();
			var partitions = GetTopicPartitions(topicName);
			if (!partitions.Any()) return Array.Empty<string>();

			var groups = GetConsumeGroups(); // 取得所有 Group 名稱列表
			var topicPartitions = partitions.ToList();

			using var adminClient = new AdminClientBuilder(new AdminClientConfig
			{
				BootstrapServers = config.BrokerHost,
				SocketTimeoutMs = config.TimeoutInMs
			}).Build();

			// 1. 建立所有的異步 Task，但不立即等待
			var tasks = groups.Select(async group =>
			{
				try
				{
					var result = await adminClient.ListConsumerGroupOffsetsAsync(
						new[] { new ConsumerGroupTopicPartitions(group, topicPartitions) },
						new ListConsumerGroupOffsetsOptions { RequestTimeout = TimeSpan.FromMilliseconds(config.TimeoutInMs) }
					);

					// 判斷該 Group 是否對此 Topic 有 Offset 紀錄
					var hasOffsets = result.Any(r => r.Partitions.Any(p => !p.Offset.IsSpecial && p.Offset != Offset.Unset));
					return hasOffsets ? group : null;
				}
				catch
				{
					return null; // 忽略個別查詢失敗
				}
			});

			// 2. 並行執行所有 Request
			var results = await Task.WhenAll(tasks);

			// 3. 過濾掉 null 並排序
			return results.Where(g => g != null).OrderBy(x => x)!;
		}

		public static bool TryGetCachedConsumeGroups(string topicName, out IReadOnlyList<string> groups)
        {
            return TopicGroupCache.TryGetValue(topicName, out groups);
        }

        public static void WarmAllConsumeGroupsCache()
        {
            if (Interlocked.Exchange(ref TopicGroupCacheInitialized, 1) == 1)
            {
                return;
            }

            _ = Task.Run(() =>
            {
                try
                {
                    BuildAllConsumeGroupsCache();
                }
                catch
                {
                    Interlocked.Exchange(ref TopicGroupCacheInitialized, 0);
                }
            });
        }

        public static void RefreshConsumeGroupsCache(string topicName)
        {
            if (string.IsNullOrWhiteSpace(topicName))
            {
				return;
            }

            topicName = topicName.Trim();
            if (IsTopicGroupCacheFresh(topicName))
            {
                return;
            }

            if (!TopicGroupRefreshInProgress.TryAdd(topicName, 0))
            {
                return;
            }

            _ = Task.Run(async() =>
            {
                try
                {
                    TopicGroupCache[topicName] = (await GetConsumeGroupsAsync(topicName)).ToList();
                    TopicGroupCacheRefreshedAt[topicName] = DateTime.UtcNow;
                }
                catch
                {
                }
                finally
                {
                    TopicGroupRefreshInProgress.TryRemove(topicName, out _);
                }
            });
        }

        public static void SetCachedConsumeGroups(string topicName, IReadOnlyList<string> groups)
        {
            if (string.IsNullOrWhiteSpace(topicName))
            {
                return;
            }

            topicName = topicName.Trim();
            TopicGroupCache[topicName] = groups
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                .ToList();
            TopicGroupCacheRefreshedAt[topicName] = DateTime.UtcNow;
        }

        private static bool IsTopicGroupCacheFresh(string topicName)
        {
            return TopicGroupCache.ContainsKey(topicName)
                && TopicGroupCacheRefreshedAt.TryGetValue(topicName, out var refreshedAt)
                && DateTime.UtcNow - refreshedAt < TopicGroupCacheTtl;
        }

        private static void BuildAllConsumeGroupsCache()
        {
            var config = ConfigService.Get();
            var groups = GetConsumeGroups().ToList();
            var topicMap = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

            using var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = config.BrokerHost,
                SocketTimeoutMs = config.TimeoutInMs
            }).Build();

            foreach (var group in groups)
            {
                try
                {
                    var result = adminClient.ListConsumerGroupOffsetsAsync(
                        new[]
                        {
                            new ConsumerGroupTopicPartitions(group, null)
                        },
                        new ListConsumerGroupOffsetsOptions
                        {
                            RequestTimeout = TimeSpan.FromMilliseconds(config.TimeoutInMs)
                        })
                        .GetAwaiter()
                        .GetResult();

                    foreach (var partition in result.SelectMany(x => x.Partitions))
                    {
                        if (partition.Offset.IsSpecial || partition.Offset == Offset.Unset)
                        {
                            continue;
                        }

                        if (!topicMap.TryGetValue(partition.Topic, out var topicGroups))
                        {
                            topicGroups = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                            topicMap[partition.Topic] = topicGroups;
                        }

                        topicGroups.Add(group);
                    }
                }
                catch
                {
                }
            }

            foreach (var entry in topicMap)
            {
                TopicGroupCache[entry.Key] = entry.Value.OrderBy(x => x).ToList();
                TopicGroupCacheRefreshedAt[entry.Key] = DateTime.UtcNow;
            }
        }

        public static async Task<string> ProduceMessageAsync(string topicName, List<string> messageList)
        {
            var config = ConfigService.Get();

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = config.BrokerHost,
                SocketTimeoutMs = config.TimeoutInMs
            };

			StringBuilder sb = new();			
            try
            {
				int idx = 0;
				foreach (var message in messageList)
				{					
					using (var p = new ProducerBuilder<string, string>(producerConfig).Build())
					{
						var result = await p.ProduceAsync(topicName, new Message<string, string>
						{
							Key = Guid.NewGuid().ToString("N"),
							Value = message
						});
						if (idx == 0)
						{
							sb.Append("from " + result.TopicPartitionOffset.ToString());
						}
						else if (idx == messageList.Count - 1)
						{
							sb.Append($" to" + result.TopicPartitionOffset.ToString());
						}
						idx++;						
					}
				}
            }
            catch (ProduceException<Null, string> e)
            {
                throw new Exception($"{e.Error.Reason}");
            }
			sb.Append($", {messageList.Count} messages was sent.");
			return sb.ToString();
        }

        public static async Task<string> ProduceCloneMessageAsync(
            string srcTopicName,
            string offset,
            string destTopicName,
            int? partition = null)
        {
            //check topic and group
            {
                var config = ConfigService.Get();

                using (var adminClient = new AdminClientBuilder(new AdminClientConfig
                {
                    BootstrapServers = config.BrokerHost,
                    SocketTimeoutMs = config.TimeoutInMs
                })
                .Build())
                {
                    try
                    {
                        var metadata = adminClient.GetMetadata(TimeSpan.FromMilliseconds(config.TimeoutInMs));
                        var srcTopic = metadata.Topics.FirstOrDefault(x => x.Topic == srcTopicName);
                        var destTopic = metadata.Topics.FirstOrDefault(x => x.Topic == destTopicName);
                        if (srcTopic == null)
                        {
                            throw new Exception($"topic {srcTopicName} was not found.");
                        }
                        if (destTopic == null)
                        {
                            throw new Exception($"topic {destTopic} was not found.");
                        }
                    }
                    catch (Exception e)
                    {
                        throw new Exception($"{e.Message}");
                    }
                }
            }

            {
                string message = GetSingleMessageByOffset(srcTopicName, int.Parse(offset), partition).Message;
                /*
                var jo = JsonObject.Parse(message);
                jo.AsObject().Remove("Id");
                jo.AsObject().Remove("CreateDate");
                message = System.Text.Json.JsonSerializer.Serialize(jo, jsonSerialzerOptions);
                */
                var config = ConfigService.Get();

                var producerConfig = new ProducerConfig
                {
                    BootstrapServers = config.BrokerHost,
                    SocketTimeoutMs = config.TimeoutInMs
                };

                try
                {
                    using (var p = new ProducerBuilder<string, string>(producerConfig).Build())
                    {
                        var result = await p.ProduceAsync(destTopicName, new Message<string, string>
                        {
                            Key = Guid.NewGuid().ToString("N"),
                            Value = message
                        });
                        return result.TopicPartitionOffset.ToString();
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    throw new Exception($"{e.Error.Reason}");
                }
            }
        }

        static JsonSerializerOptions jsonSerialzerOptions = new JsonSerializerOptions
        {
            WriteIndented = true
        };

		public static async Task DumpMessageAsync(string srcTopicName, string offset, int? partition = null)
		{
			// 1) check topic exist (原樣保留你的邏輯)
			{
				var config = ConfigService.Get();

				using var adminClient = new AdminClientBuilder(new AdminClientConfig
				{
					BootstrapServers = config.BrokerHost,
					SocketTimeoutMs = config.TimeoutInMs
				}).Build();

				var metadata = adminClient.GetMetadata(TimeSpan.FromMilliseconds(config.TimeoutInMs));
				var srcTopic = metadata.Topics.FirstOrDefault(x => x.Topic == srcTopicName);
				if (srcTopic == null)
					throw new Exception($"topic {srcTopicName} was not found.");
			}

			// 2) parse offset / range
			var (start, end, isRange) = ParseOffsetOrRange(offset);

			if (!isRange && start < 0)
			{
				await DumpMessageFromEndAsync(srcTopicName, start, partition);
				return;
			}

			// （可選）避免一次撈爆
			const int maxBatch = 1001;
			var count = (end - start) + 1;
			if (count > maxBatch)
				throw new ArgumentException($"offset range too large. Max {maxBatch} messages per call.");

			if (!isRange)
			{
				var message = GetSingleMessageByOffset(srcTopicName, start, partition).Message;
				Console.WriteLine(message);
				return;
			}

			var arr = new JsonArray();
            var partitions = ResolveTopicPartitions(srcTopicName, partition);

            foreach (var tp in partitions)
            {
                for (int i = start; i <= end; i++)
                {
                    var obj = BuildMessageJsonObject(srcTopicName, tp.Partition.Value, i);
                    if (obj != null)
                    {
                        arr.Add(obj);
                    }
                }
            }

			var json = arr.ToJsonString(new JsonSerializerOptions
			{
				WriteIndented = true
			});

			Console.WriteLine(json);
		}

		// negative offset: -1 means the newest message, -N means the N-th message counting back from the newest
		private static async Task DumpMessageFromEndAsync(string srcTopicName, int negativeOffset, int? partition)
		{
			var ranges = await GetTopicRangeOffsetAsync(srcTopicName, partition);

			if (partition.HasValue)
			{
				var tp = ranges.Keys.FirstOrDefault(x => x.Partition.Value == partition.Value);
				if (!ranges.TryGetValue(tp, out var range) || range.EndOffset is not long endOffset)
				{
					Console.WriteLine($"partition {partition}: no messages available.");
					return;
				}

				var resolvedOffset = endOffset + negativeOffset + 1;
				if (range.StartOffset.HasValue && resolvedOffset < range.StartOffset.Value)
				{
					var available = endOffset - range.StartOffset.Value + 1;
					Console.WriteLine($"partition {partition}: only {available} message(s) available (earliest offset is {range.StartOffset.Value}).");
					return;
				}

				var message = GetSingleMessageByOffset(srcTopicName, (int)resolvedOffset, partition).Message;
				Console.WriteLine(message);
				return;
			}

			var arr = new JsonArray();
			foreach (var kv in ranges)
			{
				if (kv.Value.EndOffset is not long endOffset)
				{
					continue; // empty partition
				}

				var resolvedOffset = endOffset + negativeOffset + 1;
				if (kv.Value.StartOffset.HasValue && resolvedOffset < kv.Value.StartOffset.Value)
				{
					var available = endOffset - kv.Value.StartOffset.Value + 1;
					Console.WriteLine($"partition {kv.Key.Partition.Value}: only {available} message(s) available, skipped.");
					continue;
				}

				var obj = BuildMessageJsonObject(srcTopicName, kv.Key.Partition.Value, (int)resolvedOffset);
				if (obj != null)
				{
					arr.Add(obj);
				}
			}

			var json = arr.ToJsonString(new JsonSerializerOptions
			{
				WriteIndented = true
			});

			Console.WriteLine(json);
		}

		private static JsonObject? BuildMessageJsonObject(string topicName, int partitionValue, int offsetValue)
		{
			var record = GetMessagesByOffset(topicName, offsetValue, partitionValue).SingleOrDefault();
			if (record == null)
			{
				return null;
			}

			JsonNode? payloadNode = null;
			try
			{
				payloadNode = JsonNode.Parse(record.Message);
			}
			catch
			{
			}

			var obj = new JsonObject
			{
				["partition"] = record.TopicPartitionOffset.Partition.Value,
				["offset"] = record.TopicPartitionOffset.Offset.Value,
			};

			if (payloadNode is not null)
			{
				obj["payload"] = payloadNode;
			}
			else
			{
				obj["message"] = record.Message;
			}

			return obj;
		}

		private static (int start, int end, bool isRange) ParseOffsetOrRange(string offsetText)
		{
			if (string.IsNullOrWhiteSpace(offsetText))
				throw new ArgumentException("offset is empty.");

			offsetText = offsetText.Trim();

			if (int.TryParse(offsetText, out var single))
				return (single, single, false);

			var parts = offsetText.Split('-', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
			if (parts.Length == 2
				&& int.TryParse(parts[0], out var start)
				&& int.TryParse(parts[1], out var end))
			{
				if (start > end)
					throw new ArgumentException("offset range start must be <= end.");

				return (start, end, true);
			}

			throw new ArgumentException("offset format invalid. Use '1200' or '1200-1210'.");
		}

		public static async Task FindMessageAsync(string srcTopicName, string keyword,
            DateTime? startTime, DateTime? endTime, bool showOffset, int maxCount, bool debug, string jsonPath)
        {
            //check topic and group
            {
                var config = ConfigService.Get();

                using (var adminClient = new AdminClientBuilder(new AdminClientConfig
                {
                    BootstrapServers = config.BrokerHost,
                    SocketTimeoutMs = config.TimeoutInMs
                })
                .Build())
                {
                    try
                    {
                        var metadata = adminClient.GetMetadata(TimeSpan.FromMilliseconds(config.TimeoutInMs));
                        var srcTopic = metadata.Topics.FirstOrDefault(x => x.Topic == srcTopicName);
                        if (srcTopic == null)
                        {
                            throw new Exception($"topic {srcTopicName} was not found.");
                        }
                    }
                    catch (Exception e)
                    {
                        throw new Exception($"{e.Message}");
                    }
                }
            }

            {
                List<string> messages = FindMessages(srcTopicName, keyword, startTime, endTime, showOffset, maxCount, debug);

                var config = ConfigService.Get();

                var producerConfig = new ProducerConfig
                {
                    BootstrapServers = config.BrokerHost,
                    SocketTimeoutMs = config.TimeoutInMs
                };

				// 3) 輸出
				foreach (var msg in messages)
				{
					var outputText = string.IsNullOrWhiteSpace(jsonPath)
						? msg
						: ExtractJsonValue(msg, jsonPath);

					if (string.IsNullOrEmpty(outputText))
					{
						continue;
					}
					Console.WriteLine(outputText);
				}

			}
		}

		#region find with time range

		public static async Task FindMessage2Async(
			string srcTopicName,
			string keyword,
			DateTime? startTime,
			DateTime? endTime,
			bool showOffset,
			int maxCount,
			bool debug,
			string jsonPath)
		{
			// 基本防呆
			if (string.IsNullOrWhiteSpace(srcTopicName))
				throw new ArgumentException("srcTopicName is required.", nameof(srcTopicName));

			if (maxCount <= 0) maxCount = 100;

			if (startTime.HasValue && endTime.HasValue && startTime.Value > endTime.Value)
				throw new ArgumentException("startTime must be <= endTime");

			var config = ConfigService.Get();

			// 1) 用 AdminClient 驗證 topic 存在 + 取得 partitions
			List<TopicPartition> partitions;
			{
				using var adminClient = new AdminClientBuilder(new AdminClientConfig
				{
					BootstrapServers = config.BrokerHost,
					SocketTimeoutMs = config.TimeoutInMs
				}).Build();

				var metadata = adminClient.GetMetadata(TimeSpan.FromMilliseconds(config.TimeoutInMs));
				var srcTopic = metadata.Topics.FirstOrDefault(x => x.Topic == srcTopicName);

				if (srcTopic == null)
					throw new Exception($"topic {srcTopicName} was not found.");

				if (srcTopic.Error.Code != ErrorCode.NoError)
					throw new Exception($"topic {srcTopicName} metadata error: {srcTopic.Error.Reason}");

				partitions = srcTopic.Partitions
					.Select(p => new TopicPartition(srcTopicName, new Partition(p.PartitionId)))
					.ToList();

				if (partitions.Count == 0)
					throw new Exception($"topic {srcTopicName} has no partitions.");
			}

			// 2) 讀取訊息（時間定位 + 範圍掃描）
			var messages = await Task.Run(() =>
				FindMessagesByTimeRange(
					config,
					srcTopicName,
					partitions,
					keyword,
					startTime,
					endTime,
					showOffset,
					maxCount,
					debug));

			// 3) 輸出
			foreach (var msg in messages)
			{
				var outputText = string.IsNullOrWhiteSpace(jsonPath)
					? msg
					: ExtractJsonValue(msg, jsonPath);

				if (string.IsNullOrEmpty(outputText))
				{
					continue;
				}
				Console.WriteLine(outputText);
			}

		}

		private static List<string> FindMessagesByTimeRange(
			dynamic config, // 保留你原本的 ConfigService.Get() 型別；你可改成明確型別
			string topic,
			List<TopicPartition> partitions,
			string keyword,
			DateTime? startTime,
			DateTime? endTime,
			bool showOffset,
			int maxCount,
			bool debug)
		{
			var consumerConfig = new ConsumerConfig
			{
				BootstrapServers = config.BrokerHost,
				GroupId = $"find-msg-{Guid.NewGuid():N}",
				EnableAutoCommit = false,
				AutoOffsetReset = AutoOffsetReset.Earliest,
				SocketTimeoutMs = config.TimeoutInMs,

				// 讀大量訊息時可稍微加大 fetch（視環境調整）
				FetchMaxBytes = 50_000_000,
				MaxPartitionFetchBytes = 10_000_000,
			};

			using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();

			// 重點：用 Assign 才能精準控制每個 partition 的 Seek
			consumer.Assign(partitions);

			// 用 UTC 比較比較安全（避免本地時區混亂）
			DateTime? startUtc = startTime?.ToUniversalTime();
			DateTime? endUtc = endTime?.ToUniversalTime();

			// 3) 計算 start/end offsets
			Dictionary<TopicPartition, long?> startOffsets = ResolveOffsetsForTime(consumer, partitions, startUtc, config.TimeoutInMs);
			Dictionary<TopicPartition, long?> endOffsets = ResolveOffsetsForTime(consumer, partitions, endUtc, config.TimeoutInMs);

			// 4) Seek 到 start offset（若找不到就從 Beginning；你也可以改成「該 partition 直接 done」）
			foreach (var tp in partitions)
			{
				var off = startOffsets.TryGetValue(tp, out var so) ? so : null;

				if (off.HasValue)
					consumer.Seek(new TopicPartitionOffset(tp, new Offset(off.Value)));
				else
					consumer.Seek(new TopicPartitionOffset(tp, Offset.Beginning));

				if (debug)
					Console.WriteLine($"[DEBUG] Seek start {tp}: {(off.HasValue ? off.Value.ToString() : "Beginning")}");
			}

			// 5) 範圍讀取
			var results = new List<string>(capacity: Math.Min(maxCount, 1000));

			// 每個 partition 是否已完成（超過 endTime 或到 endOffset）
			var done = partitions.ToDictionary(tp => tp, _ => false);

			// 避免無限跑：給個總超時，你可自行調整
			using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

			while (!cts.IsCancellationRequested && results.Count < maxCount)
			{
				if (done.Values.All(x => x)) break;

				var cr = consumer.Consume(TimeSpan.FromMilliseconds(200));
				if (cr == null) continue;

				var tp = cr.TopicPartition;
				if (!done.ContainsKey(tp) || done[tp]) continue;

				// (A) endOffset 硬上界（OffsetsForTimes(endTime) 回來的是「>= endTime 的第一筆」附近）
				//     若你希望 endTime 以「包含」為主，下面用 timestamp 檢查會比較準
				if (endOffsets.TryGetValue(tp, out var eo) && eo.HasValue && cr.Offset.Value >= eo.Value)
				{
					// 不一定代表已超過 endTime（eo 可能是 unset 或跳點），所以我們仍建議用 timestamp 再判斷一次。
					// 這裡先不直接 done，改用 timestamp 判斷為主；若你追求更快可直接 done。
				}

				// (B) 用訊息 timestamp 判斷時間範圍（比較準）
				var msgUtc = cr.Message.Timestamp.UtcDateTime;

				if (startUtc.HasValue && msgUtc < startUtc.Value)
					continue;

				if (endUtc.HasValue && msgUtc > endUtc.Value)
				{
					done[tp] = true;
					if (debug)
						Console.WriteLine($"[DEBUG] Done {tp} because msg ts {msgUtc:o} > end {endUtc:o}");
					continue;
				}

				// (C) keyword match
				if (!string.IsNullOrEmpty(keyword) || keyword == "*")
				{
					var val = cr.Message.Value ?? string.Empty;
					if (!val.Contains(keyword, StringComparison.OrdinalIgnoreCase))
						continue;
				}

				// (D) 收集結果
				string line;
				if (showOffset)
				{
					line = $"[{cr.TopicPartition}] offset={cr.Offset.Value} ts={msgUtc:o} {cr.Message.Value}";
				}
				else
				{
					line = cr.Message.Value;
				}

				results.Add(line);

				if (debug)
					Console.WriteLine($"[DEBUG] HIT {tp} offset={cr.Offset.Value} ts={msgUtc:o}");
			}

			if (debug)
				Console.WriteLine($"[DEBUG] Finished. hits={results.Count}");

			return results;
		}

		private static string ExtractJsonValue(string jsonText, string outputPath)
		{
			if (string.IsNullOrWhiteSpace(outputPath))
			{
				return jsonText;
			}

			JsonNode? node;
			try
			{
				node = JsonNode.Parse(jsonText);
			}
			catch
			{
				// 不是合法 JSON，就直接回原文
				return jsonText;
			}

			if (node == null)
			{
				return jsonText;
			}

			var current = node;
			var parts = outputPath.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

			foreach (var part in parts)
			{
				if (current is JsonObject obj)
				{
					if (!obj.TryGetPropertyValue(part, out current) || current == null)
					{
						return string.Empty;
					}
				}
				else
				{
					return string.Empty;
				}
			}

			if (current is JsonValue value)
			{
				return value.ToString();
			}

			return current.ToJsonString(new JsonSerializerOptions
			{
				WriteIndented = false
			});
		}

		/// <summary>
		/// 回傳每個 partition 在指定時間點的 offset（可能為 null 代表 Offset.Unset）
		/// </summary>
		private static Dictionary<TopicPartition, long?> ResolveOffsetsForTime(
			IConsumer<Ignore, string> consumer,
			List<TopicPartition> partitions,
			DateTime? utcTime,
			int timeoutMs)
		{
			var map = partitions.ToDictionary(tp => tp, _ => (long?)null);

			if (!utcTime.HasValue)
				return map;

			var tpts = partitions
				.Select(tp => new TopicPartitionTimestamp(tp, new Timestamp(utcTime.Value, TimestampType.CreateTime)))
				.ToList();

			var results = consumer.OffsetsForTimes(tpts, TimeSpan.FromMilliseconds(timeoutMs));

			foreach (var r in results)
			{
				if (r.Offset == Offset.Unset)
				{
					map[r.TopicPartition] = null;
				}
				else
				{
					map[r.TopicPartition] = r.Offset.Value;
				}
			}

			return map;
		}

		#endregion find with time range

		public static async Task FindManyMessageAsync(string srcTopicName, string[] keywords, int maxCount)
        {
            //check topic and group
            {
                var config = ConfigService.Get();

                using (var adminClient = new AdminClientBuilder(new AdminClientConfig
                {
                    BootstrapServers = config.BrokerHost,
                    SocketTimeoutMs = config.TimeoutInMs
                })
                .Build())
                {
                    try
                    {
                        var metadata = adminClient.GetMetadata(TimeSpan.FromMilliseconds(config.TimeoutInMs));
                        var srcTopic = metadata.Topics.FirstOrDefault(x => x.Topic == srcTopicName);
                        if (srcTopic == null)
                        {
                            throw new Exception($"topic {srcTopicName} was not found.");
                        }
                    }
                    catch (Exception e)
                    {
                        throw new Exception($"{e.Message}");
                    }
                }
            }

            {
                List<string> messages = DoFindManyMessages(srcTopicName, keywords, maxCount);

                var config = ConfigService.Get();

                var producerConfig = new ProducerConfig
                {
                    BootstrapServers = config.BrokerHost,
                    SocketTimeoutMs = config.TimeoutInMs
                };

                messages.ForEach(x => Console.WriteLine(x));
                Console.WriteLine($"共 {messages.Count} 筆");
            }
        }

        public static async Task FindMessagesByJsonPathAsync(
            string srcTopicName,
            string jsonPath,
            int startOffset,
            int? endOffset,
            int maxResult,
            bool showComment,
            int? partition = null)
        {
            //check topic and group
            {
                var config = ConfigService.Get();

                using (var adminClient = new AdminClientBuilder(new AdminClientConfig
                {
                    BootstrapServers = config.BrokerHost,
                    SocketTimeoutMs = config.TimeoutInMs
                })
                .Build())
                {
                    try
                    {
                        var metadata = adminClient.GetMetadata(TimeSpan.FromMilliseconds(config.TimeoutInMs));
                        var srcTopic = metadata.Topics.FirstOrDefault(x => x.Topic == srcTopicName);
                        if (srcTopic == null)
                        {
                            throw new Exception($"topic {srcTopicName} was not found.");
                        }
                    }
                    catch (Exception e)
                    {
                        throw new Exception($"{e.Message}");
                    }
                }
            }

            {
                System.Diagnostics.Stopwatch watch = new System.Diagnostics.Stopwatch();
                watch.Start();

                List<string> messages = DoFindMessagesByJsonPath(
                    srcTopicName, jsonPath, startOffset, endOffset, maxResult, showComment, partition);

                var config = ConfigService.Get();

                var producerConfig = new ProducerConfig
                {
                    BootstrapServers = config.BrokerHost,
                    SocketTimeoutMs = config.TimeoutInMs
                };

                messages.ForEach(x => Console.WriteLine(x));

                string endOffsetStr = endOffset == null ? "∞" : endOffset.Value.ToString();
                Console.WriteLine("共 {0} 筆, offset {1}-{2}, took {3}s.",
                    messages.Count, startOffset, endOffsetStr, watch.Elapsed.TotalSeconds.ToString("0.00"));
            }
        }

        private static List<string> DoFindMessagesByJsonPath(
            string topicName,
            string jsonPath,
            int startOffset,
            int? endOffset,
            int maxResult,
            bool showComment,
            int? partition = null)
        {
            var config = ConfigService.Get();

            var conf = new ConsumerConfig
            {
                BootstrapServers = config.BrokerHost,
                GroupId = "temp-" + Guid.NewGuid().ToString().ToLower(),
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            List<string> result = new List<string>();
            var partitions = ResolveTopicPartitions(topicName, partition);

            using (IConsumer<Ignore, string> _consumer = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                try
                {
                    var assignments = partitions
                        .Select(tp => new TopicPartitionOffset(tp, new Offset(startOffset)))
                        .ToList();
                    _consumer.Assign(assignments);

                    ConsumeResult<Ignore, string> cr;
                    while (true)
                    {
                        cr = _consumer.Consume(config.TimeoutInMs);
                        if (cr == null)
                        {
                            break;
                        }
                        if (cr.Offset < startOffset)
                        {
                            continue;
                        }
                        if (endOffset != null && cr.Offset > endOffset)
                        {
                            continue;
                        }

                        {
                            string messageTxt = cr.Message.Value;
                            JsonElement jsonObject;
                            try
                            {
                                jsonObject = JsonDocument.Parse(messageTxt).RootElement;
                            }
                            catch (Exception ex)
                            {
                                throw new Exception($"Parse error.offset={cr.Offset}", ex);
                            }
                            string jsonElemValue;
                            try
                            {
                                jsonElemValue = JsonHelper.FindByJsonPath(jsonObject, jsonPath);
                            }
                            catch (Exception ex)
                            {
                                throw new Exception($"FindByJsonPath error.offset={cr.Offset}", ex);
                            }
                            if (string.IsNullOrEmpty(jsonElemValue) == false)
                            {
                                if (showComment)
                                {
                                    result.Add($"{cr.Partition.Value}:{cr.Offset.Value}:{jsonElemValue}");
                                }
                                else
                                {
                                    result.Add(jsonElemValue);
                                }
                            }

                            if (result.Count >= maxResult)
                            {
                                break;
                            }
                        }
                    }

                    _consumer.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    _consumer.Close();
                }
            }
            return result;
        }

        public static async Task SetConsumerOffset(
            string topicName,
            string groupId,
            long offset,
            bool fromCurrent,
            int? partition = null)
        {
            var config = ConfigService.Get();

            var conf = new ConsumerConfig
            {
                BootstrapServers = config.BrokerHost,
                GroupId = groupId,
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var topicRangeOffsets = await GetTopicRangeOffsetAsync(topicName, partition);
            
            using (var _consumer = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                try
                {
                    var partitions = topicRangeOffsets.Keys.ToList();
                    var committedOffsets = _consumer.Committed(partitions, TimeSpan.FromMilliseconds(config.TimeoutInMs));
                    var commitTargets = new List<TopicPartitionOffset>();

                    foreach (var committed in committedOffsets)
                    {
                        var range = topicRangeOffsets[committed.TopicPartition];
                        long offsetValue = fromCurrent
                            ? committed.Offset.Value + offset
                            : offset;

                        if (range.StartOffset.HasValue && offsetValue < range.StartOffset.Value)
                        {
                            Console.WriteLine($"partition {committed.Partition.Value}: below the minimum range.");
                            continue;
                        }

                        if (range.EndOffset.HasValue && offsetValue - 1 > range.EndOffset.Value)
                        {
                            Console.WriteLine($"partition {committed.Partition.Value}: exceeded maximum range.");
                            continue;
                        }

                        commitTargets.Add(new TopicPartitionOffset(committed.TopicPartition, new Offset(offsetValue)));
                    }

                    if (commitTargets.Count == 0)
                    {
                        return;
                    }

                    _consumer.Commit(commitTargets);

                    _consumer.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    _consumer.Close();
                }
            }
        }

        public static string GetMessage(string topicName, int offset, int? partition = null)
        {
            return GetSingleMessageByOffset(topicName, offset, partition).Message;
        }

        private static List<KeywordOperatorPair> ParseKeyword(string input)
        {
            if (input != null && input[0] != '+' && input[0] != '-')
            {
                input = "+" + input;
            }
            // 用於分割字串並保留分隔符的正規表達式
            var matches = Regex.Matches(input, @"([\w ]+)|([+-])");

            List<KeywordOperatorPair> result = new();
            string lastOperator = "+"; // 初始假設第一個數字前有一個隱含的 '+'

            foreach (Match match in matches)
            {
                if (match.Value == "+" || match.Value == "-")
                {
                    lastOperator = match.Value;
                }
                else
                {
                    // 加入前一個運算符和當前匹配的數字或字母
                    result.Add(new KeywordOperatorPair { Operator = lastOperator, Keyword = match.Value });
                }
            }

            return result;
        }

        private static List<string> FindMessages(
            string topicName,
            string keywordLine,
            DateTime? startTime,
            DateTime? endTime,
            bool showOffset,
            int maxCount,
			bool debug)
        {
            var config = ConfigService.Get();

            var conf = new ConsumerConfig
            {
                BootstrapServers = config.BrokerHost,
                GroupId = "temp-" + Guid.NewGuid().ToString().ToLower(),
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

			// 先把時間統一轉成 UTC，避免迴圈中重複轉換
			DateTime? startUtc = startTime.HasValue
				? DateTime.SpecifyKind(startTime.Value, DateTimeKind.Local).ToUniversalTime()
				: (DateTime?)null;
			DateTime? endUtc = endTime.HasValue
				? DateTime.SpecifyKind(endTime.Value, DateTimeKind.Local).ToUniversalTime()
				: (DateTime?)null;

			List<string> result = new List<string>();
            var keywordPairs = ParseKeyword(keywordLine);
            var partitions = ResolveTopicPartitions(topicName, null);
            using (IConsumer<Ignore, string> _consumer = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                try
                {
					if (startUtc.HasValue)
					{
						var offsets = _consumer.OffsetsForTimes(
							partitions.Select(tp => new TopicPartitionTimestamp(tp, new Timestamp(startUtc.Value))).ToList(),
							TimeSpan.FromMilliseconds(config.TimeoutInMs));
                        var assignments = new List<TopicPartitionOffset>();
                        foreach (var off in offsets)
                        {
                            var tp = off.TopicPartition;
                            if (!off.Offset.IsSpecial)
                            {
                                if (debug) Console.WriteLine($"Partition {tp.Partition.Value} start from offset {off.Offset}");
                                assignments.Add(new TopicPartitionOffset(tp, off.Offset));
                                continue;
                            }

                            var wm = _consumer.QueryWatermarkOffsets(tp, TimeSpan.FromMilliseconds(config.TimeoutInMs));
                            if (off.Offset == Offset.End)
                            {
                                if (debug) Console.WriteLine($"Partition {tp.Partition.Value} start from offset end");
                                assignments.Add(new TopicPartitionOffset(tp, wm.High));
                            }
                            else if (off.Offset == Offset.Beginning)
                            {
                                if (debug) Console.WriteLine($"Partition {tp.Partition.Value} start from offset begin");
                                assignments.Add(new TopicPartitionOffset(tp, wm.Low));
                            }
                            else
                            {
                                if (debug) Console.WriteLine($"Partition {tp.Partition.Value} start from offset low");
                                assignments.Add(new TopicPartitionOffset(tp, wm.Low));
                            }
                        }
                        _consumer.Assign(assignments);
					}
					else
					{
						_consumer.Assign(partitions);
					}
					bool timestampTypeOutput = false;
					ConsumeResult<Ignore, string> cr;
                    while (true)
                    {
                        cr = _consumer.Consume(config.TimeoutInMs);
                        if (cr == null)
                        {
                            break;
                        }

						if (startTime.HasValue || endTime.HasValue)
						{
							var ts = cr.Message.Timestamp;
							if (debug && !timestampTypeOutput)
							{
								timestampTypeOutput = true;
								Console.WriteLine($"TimestampType {ts.Type.ToString()}");
							}
							if (ts.Type != TimestampType.NotAvailable)
							{
								var utc = ts.UtcDateTime;

								if (startUtc.HasValue && utc < startUtc.Value) continue;
								if (endUtc.HasValue && utc > endUtc.Value) continue;
							}
							else
							{
								DateTime? messageTime = null;
								if (Helper.TryExtractCreateOn(cr.Message.Value, out messageTime))
								{
									if (startTime.HasValue && messageTime < startTime)
									{
										continue;
									}
									if (endTime.HasValue && messageTime > endTime)
									{
										continue;
									}
								}
							}
						}

						bool allMatch = true;
                        string utf8Message = Helper.UnicodeToString(cr.Message.Value);
                        foreach (var keywordPair in keywordPairs)
                        {
							if (keywordPair.Keyword == "*")
							{
								break;
							}
                            var keyword = keywordPair.Keyword;
                            var op = keywordPair.Operator;
                            if (op == "+" && utf8Message.Contains(keyword) == false)
                            {
                                allMatch = false;
                            }
                            else if (op == "-" && utf8Message.Contains(keyword))
                            {
                                allMatch = false;
                            }
                        }
                        if (allMatch)
                        {
                            string messageTxt = utf8Message;
                            if (showOffset)
                            {
                                messageTxt += $"/*p{cr.Partition.Value}:{cr.Offset}*/";
                            }

                            result.Add(messageTxt);
                            if (result.Count >= maxCount)
                            {
                                break;
                            }
                        }
                    }                    
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);                    
                }
				finally
				{
					try { _consumer.Close(); } catch { /* ignore */ }
				}
			}
            return result;
        }

        private static List<string> DoFindManyMessages(
            string topicName,
            string[] keywords,
            int maxCount)
        {
            var config = ConfigService.Get();

            var conf = new ConsumerConfig
            {
                BootstrapServers = config.BrokerHost,
                GroupId = "temp-" + Guid.NewGuid().ToString().ToLower(),
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            List<string> result = new List<string>();
            var partitions = ResolveTopicPartitions(topicName, null);

            using (IConsumer<Ignore, string> _consumer = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                try
                {
                    _consumer.Assign(partitions);

                    ConsumeResult<Ignore, string> cr;					
                    while (true)
                    {
                        cr = _consumer.Consume(config.TimeoutInMs);
                        if (cr == null)
                        {
                            break;
                        }

                        foreach (var keyword in keywords)
                        {
                            if (cr.Message.Value.Contains(keyword))
                            {
                                string messageTxt = cr.Message.Value;

                                // 解析 JSON 字符串
                                var jsonObject = JsonSerializer.Deserialize<JsonElement>(messageTxt);
                                // 創建序列化選項，禁用縮排
                                var options = new JsonSerializerOptions
                                {
                                    WriteIndented = false,
                                    Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
                                };
                                // 將 JSON 對象轉換回字符串，不帶縮排
                                string formattedJson = JsonSerializer.Serialize(jsonObject, options);

                                result.Add(formattedJson);
                                break;
                            }
                        }
                        if (result.Count >= maxCount)
                        {
                            break;
                        }
                    }

                    _consumer.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    _consumer.Close();
                }
            }
            return result;
        }

        public static void ConsumeMessage(
            string topicName,
            string consumerGroupId,
            bool commit,
            Action<string, string> messageHandler,
            Action<string> errorHandler)
        {
            var config = ConfigService.Get();

            var conf = new ConsumerConfig
            {
                GroupId = consumerGroupId,
                BootstrapServers = config.BrokerHost,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = commit
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe(topicName);
                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            messageHandler(cr.TopicPartitionOffset.ToString(), cr.Value);
                        }
                        catch (ConsumeException e)
                        {
                            errorHandler(e.Error.Reason);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            }
        }

        private static string RemoveControlCharacters(string inString)
        {
            if (inString == null) return null;
            StringBuilder newString = new StringBuilder();
            char ch;
            for (int i = 0; i < inString.Length; i++)
            {
                ch = inString[i];
                if (!char.IsControl(ch))
                {
                    newString.Append(ch);
                }
            }
            return newString.ToString();
        }

		public static async Task RemoteCopyMessageAsync(
			string topicName,
			string sourceHost,
			string targetHost,
			long? startOffset,
			long? endOffset,
            int? partition = null)
		{
			var config = ConfigService.Get();

			if (startOffset.HasValue && endOffset.HasValue && startOffset > endOffset)
			{
				throw new ArgumentException("startOffset cannot be greater than endOffset.");
			}

			var conf = new ConsumerConfig
			{
				BootstrapServers = sourceHost ?? config.BrokerHost,
				GroupId = "temp-" + Guid.NewGuid().ToString().ToLower(),
				EnableAutoCommit = false,
				AutoOffsetReset = AutoOffsetReset.Earliest
			};

			var messages = new List<string>();
            var partitions = ResolveTopicPartitions(topicName, partition, sourceHost ?? config.BrokerHost);

			using (var consumer = new ConsumerBuilder<Ignore, string>(conf).Build())
			{
				try
				{
                    if (startOffset.HasValue)
                    {
                        consumer.Assign(partitions.Select(tp => new TopicPartitionOffset(tp, new Offset(startOffset.Value))).ToList());
                    }
                    else
                    {
                        consumer.Assign(partitions);
                    }

					while (true)
					{
						var cr = consumer.Consume(TimeSpan.FromMilliseconds(10000));
						if (cr == null)
						{
							// 在 timeout 期間沒有訊息，結束
							break;
						}

						// 如果有指定 endOffset，超過範圍就中止
						if (endOffset.HasValue && cr.Offset.Value > endOffset.Value)
						{
							break;
						}

						// 理論上如果從 startOffset 開始 assign，不會遇到小於 startOffset 的情況
						// 但這裡保險再過濾一次
						if (startOffset.HasValue && cr.Offset.Value < startOffset.Value)
						{
							continue;
						}

						string utf8Message = Helper.UnicodeToString(cr.Message.Value);
						messages.Add(utf8Message);
						if (messages.Count % 100 == 0)
						{
							Console.WriteLine($"read {messages.Count} messages");
						}
					}
					Console.WriteLine($"read {messages.Count} messages");
					Console.WriteLine("read done");
				}
				catch (Exception ex)
				{
					Console.WriteLine($"[CopyMessageAsync] Error: {ex}");
				}
				finally
				{
					try { consumer.Close(); } catch { /* ignore */ }
				}
			}

			var producerConfig = new ProducerConfig
			{
				BootstrapServers = targetHost ?? config.BrokerHost,
				SocketTimeoutMs = config.TimeoutInMs
			};

			try
			{
				int idx = 0;
				using (var p = new ProducerBuilder<string, string>(producerConfig).Build())
				{
					foreach (var message in messages)
					{
						var result = await p.ProduceAsync(topicName, new Message<string, string>
						{
							Key = Guid.NewGuid().ToString("N"),
							Value = message
						});						
						idx++;
						if (idx % 100 == 0)
						{
							Console.WriteLine($"write {idx}/{messages.Count} messages");
						}
					}
				}
				Console.WriteLine($"write {idx}/{messages.Count} messages");
				Console.WriteLine("weite done");
			}
			catch (ProduceException<string, string> e)
			{
				throw new Exception($"{e.Error.Reason}");
			}
		}
	}

    public class KeywordOperatorPair
    {
        public string Operator { get; set; }
        public string Keyword { get; set; }
    }
}
