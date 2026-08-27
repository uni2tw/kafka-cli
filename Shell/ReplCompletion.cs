using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Tool.Cli.Kafka;

namespace Kafka.Tool.Cli.Shell
{
    internal sealed class ReplCompletion
    {
        private static readonly string[] RootCommands =
        {
            "help", "history", "context", "clear", "cls", "exit", "quit",
            "use", "topic", "message", "consumer", "config",
            "get", "find", "groups", "offset"
        };

        private static readonly string[] MessageCommands = { "produce", "consume", "get", "find", "find-many", "find-path", "clone", "remote-copy" };
        private static readonly string[] ConsumerCommands = { "groups", "offset" };
        private static readonly string[] UseCommands = { "group", "clear" };
        private static readonly string[] CommonOptions =
        {
            "-t", "--topic", "-g", "--group", "-p", "--partition",
            "-o", "--offset", "-so", "--start-offset", "-eo", "--end-offset",
            "-n", "--ntop", "-s", "--start", "-e", "--end", "-sc",
            "-sh", "--source-host", "-th", "--target-host"
        };
        private static readonly string[] TopicListOptions = { "-f", "--filter" };
        private static readonly string[] MessageGetOptions = { "-t", "--topic", "-p", "--partition", "-o", "--offset" };
        private static readonly string[] MessageFindOptions = { "-t", "--topic", "-n", "--ntop", "-s", "--start", "-e", "--end", "-d", "--debug", "-b", "--beta", "-p", "--path", "-so" };
        private static readonly string[] MessageFindPathOptions = { "-t", "--topic", "-so", "--start-offset", "-eo", "--end-offset", "-mr", "--max-result", "-sc", "-p", "--partition" };
        private static readonly string[] MessageConsumeOptions = { "-t", "--topic", "-g", "--group", "-c", "--commit", "-p", "--pause" };
        private static readonly string[] MessageCloneOptions = { "-t", "--topic", "-o", "--offset", "-tt", "--to-topic", "-p", "--partition" };
        private static readonly string[] MessageRemoteCopyOptions = { "-t", "--topic", "-sh", "--source-host", "-th", "--target-host", "-so", "--start-offset", "-eo", "--end-offset", "-p", "--partition" };
        private static readonly string[] ConsumerGroupsOptions = { "-t", "--topic" };
        private static readonly string[] ConsumerOffsetOptions = { "-t", "--topic", "-g", "--group", "-ofc", "--offsetFromCurrent", "-o", "--offset", "-p", "--partition" };

        public IReadOnlyList<string> GetCandidates(string input, ReplContext context)
        {
            bool trailingSpace = input.Length > 0 && char.IsWhiteSpace(input[^1]);
            var tokens = ReplCommandLine.TokenizePartial(input);
            string currentToken = trailingSpace ? string.Empty : tokens.LastOrDefault() ?? string.Empty;
            IReadOnlyList<string> completedTokens = trailingSpace
                ? tokens
                : tokens.Take(Math.Max(0, tokens.Count - 1)).ToArray();

            completedTokens = ReplCommandAliases.NormalizeForCompletion(completedTokens);

            IEnumerable<string> candidates = ResolveCandidates(completedTokens, context);
            if (!string.IsNullOrEmpty(currentToken))
            {
                candidates = candidates.Where(x => MatchesToken(x, currentToken));
            }

            return candidates
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        /// <summary>
        /// Matches when the candidate starts with the typed token, or when any '.'-separated
        /// segment of the candidate starts with it (e.g. typing "Internal" matches "PX.Internal.Stocks").
        /// Candidates without a dot behave exactly like a plain StartsWith.
        /// </summary>
        private static bool MatchesToken(string candidate, string currentToken)
        {
            if (candidate.StartsWith(currentToken, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            foreach (var segment in candidate.Split('.'))
            {
                if (segment.StartsWith(currentToken, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            return false;
        }

        private IEnumerable<string> ResolveCandidates(IReadOnlyList<string> tokens, ReplContext context)
        {
            if (tokens.Count == 0)
            {
                return RootCommands;
            }

            var normalizedTokens = ReplCommandAliases.NormalizeForCompletion(tokens);

            if (tokens.Count == 1)
            {
                if (tokens[0].Equals("use", StringComparison.OrdinalIgnoreCase))
                {
                    return UseCommands;
                }

                if (tokens[0].Equals("topic", StringComparison.OrdinalIgnoreCase))
                {
                    return TopicListOptions.Concat(GetTopicCandidates(context));
                }

                return tokens[0].ToLowerInvariant() switch
                {
                    "message" => MessageCommands,
                    "consumer" => ConsumerCommands,
                    "groups" => ConsumerCommands,
                    "offset" => ConsumerCommands,
                    _ => RootCommands
                };
            }

            if (IsExpectingTopicValue(normalizedTokens))
            {
                return GetTopicCandidates(context);
            }

            if (IsExpectingGroupValue(normalizedTokens))
            {
                return GetGroupCandidates(normalizedTokens, context, forceTopicFilter: true);
            }

            if (IsExpectingPartitionValue(normalizedTokens))
            {
                return GetPartitionCandidates(normalizedTokens, context);
            }

            if (tokens[0].Equals("topic", StringComparison.OrdinalIgnoreCase))
            {
                return TopicListOptions.Concat(GetTopicCandidates(context));
            }

            if (tokens[0].Equals("message", StringComparison.OrdinalIgnoreCase))
            {
                return GetMessageCommandCandidates(tokens);
            }

            if (tokens[0].Equals("use", StringComparison.OrdinalIgnoreCase))
            {
                return tokens[1].ToLowerInvariant() switch
                {
                    "group" => GetGroupCandidates(tokens, context, forceTopicFilter: false),
                    _ => UseCommands
                };
            }

            if (tokens[0].Equals("consumer", StringComparison.OrdinalIgnoreCase))
            {
                return GetConsumerCommandCandidates(tokens);
            }

            if (tokens[0].Equals("get", StringComparison.OrdinalIgnoreCase))
            {
                return MessageGetOptions.Concat(GetTopicCandidates(context));
            }

            if (tokens[0].Equals("find", StringComparison.OrdinalIgnoreCase))
            {
                return MessageFindOptions.Concat(GetTopicCandidates(context));
            }

            if (tokens[0].Equals("groups", StringComparison.OrdinalIgnoreCase))
            {
                return GetConsumerCommandCandidates(normalizedTokens);
            }

            return RootCommands.Concat(CommonOptions);
        }

        private IEnumerable<string> GetMessageCommandCandidates(IReadOnlyList<string> tokens)
        {
            if (tokens.Count == 1)
            {
                return MessageCommands;
            }

            if (tokens.Count == 2)
            {
                return MessageCommands;
            }

            return tokens[1].ToLowerInvariant() switch
            {
                "get" => MessageGetOptions.Concat(GetTopicCandidates(null)),
                "find" => MessageFindOptions.Concat(GetTopicCandidates(null)),
                "find-many" => MessageFindOptions.Concat(GetTopicCandidates(null)),
                "find-path" => MessageFindPathOptions.Concat(GetTopicCandidates(null)),
                "consume" => MessageConsumeOptions.Concat(GetTopicCandidates(null)).Concat(GetGroupCandidates(tokens, null, forceTopicFilter: false)),
                "clone" => MessageCloneOptions.Concat(GetTopicCandidates(null)),
                "remote-copy" => MessageRemoteCopyOptions.Concat(GetTopicCandidates(null)),
                _ => MessageCommands.Concat(CommonOptions)
            };
        }

        private IEnumerable<string> GetConsumerCommandCandidates(IReadOnlyList<string> tokens)
        {
            if (tokens.Count == 1)
            {
                return ConsumerCommands;
            }

            if (tokens.Count == 2)
            {
                return ConsumerCommands;
            }

            return tokens[1].ToLowerInvariant() switch
            {
                "groups" => ConsumerGroupsOptions.Concat(GetTopicCandidates(null)),
                "offset" => ConsumerOffsetOptions.Concat(GetTopicCandidates(null)).Concat(GetGroupCandidates(tokens, null, forceTopicFilter: false)),
                _ => ConsumerCommands.Concat(CommonOptions)
            };
        }

        private static bool IsExpectingTopicValue(IReadOnlyList<string> tokens)
        {
            var last = tokens[^1];
            return last.Equals("-t", StringComparison.OrdinalIgnoreCase)
                || last.Equals("--topic", StringComparison.OrdinalIgnoreCase);
        }

        private static bool IsExpectingPartitionValue(IReadOnlyList<string> tokens)
        {
            var last = tokens[^1];
            return last.Equals("-p", StringComparison.OrdinalIgnoreCase)
                || last.Equals("--partition", StringComparison.OrdinalIgnoreCase);
        }

        private static bool IsExpectingGroupValue(IReadOnlyList<string> tokens)
        {
            var last = tokens[^1];
            if (last.Equals("-g", StringComparison.OrdinalIgnoreCase)
                || last.Equals("--group", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            return tokens.Count >= 2
                && tokens[0].Equals("use", StringComparison.OrdinalIgnoreCase)
                && tokens[1].Equals("group", StringComparison.OrdinalIgnoreCase);
        }

        private static IEnumerable<string> GetTopicCandidates(ReplContext context)
        {
            var result = new List<string>();
            if (context != null && !string.IsNullOrWhiteSpace(context.DefaultTopic))
            {
                result.Add(context.DefaultTopic);
            }

            try
            {
                result.AddRange(KafkaClient.ListTopics());
            }
            catch
            {
            }

            return result;
        }

        private static IEnumerable<string> GetGroupCandidates(IReadOnlyList<string> tokens, ReplContext context, bool forceTopicFilter)
        {
            var result = new List<string>();
            if (context != null && !string.IsNullOrWhiteSpace(context.DefaultGroup))
            {
                result.Add(context.DefaultGroup);
            }

            var topic = TryGetOptionValue(tokens, "-t", "--topic");
            if (string.IsNullOrWhiteSpace(topic))
            {
                topic = context != null ? context.DefaultTopic : null;
            }

            try
            {
                if (!string.IsNullOrWhiteSpace(topic))
                {
					if (KafkaClient.TryGetCachedConsumeGroups(topic, out var cachedGroups))
                    {
                        result.AddRange(cachedGroups);
                    }
                    else
                    {
                        KafkaClient.RefreshConsumeGroupsCache(topic);
                        if (forceTopicFilter)
                        {
                            return result;
                        }

                        result.AddRange(KafkaClient.GetConsumeGroups());
                    }
                }
                else
                {
                    result.AddRange(KafkaClient.GetConsumeGroups());
                }
            }
            catch
            {
            }

            return result;
        }

        private static IEnumerable<string> GetPartitionCandidates(IReadOnlyList<string> tokens, ReplContext context)
        {
            var topic = TryGetOptionValue(tokens, "-t", "--topic");
            if (string.IsNullOrWhiteSpace(topic))
            {
                topic = context != null ? context.DefaultTopic : null;
            }

            if (string.IsNullOrWhiteSpace(topic))
            {
                return Array.Empty<string>();
            }

            try
            {
                return KafkaClient.GetTopicPartitions(topic)
                    .Select(x => x.Partition.Value.ToString());
            }
            catch
            {
                return Array.Empty<string>();
            }
        }

        private static string TryGetOptionValue(IReadOnlyList<string> tokens, params string[] options)
        {
            for (int i = 0; i < tokens.Count - 1; i++)
            {
                foreach (var option in options)
                {
                    if (tokens[i].Equals(option, StringComparison.OrdinalIgnoreCase))
                    {
                        return tokens[i + 1];
                    }
                }
            }

            return null;
        }
    }
}
