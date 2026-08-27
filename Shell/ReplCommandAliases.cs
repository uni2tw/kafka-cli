using System;
using System.Collections.Generic;

namespace Kafka.Tool.Cli.Shell
{
    internal static class ReplCommandAliases
    {
        public static IReadOnlyList<string> NormalizeForExecution(IReadOnlyList<string> tokens)
        {
            return Normalize(tokens, includeGetFind: true, includeGroups: true, includeOffset: true);
        }

        public static IReadOnlyList<string> NormalizeForCompletion(IReadOnlyList<string> tokens)
        {
            return Normalize(tokens, includeGetFind: true, includeGroups: true, includeOffset: true);
        }

        private static IReadOnlyList<string> Normalize(
            IReadOnlyList<string> tokens,
            bool includeGetFind,
            bool includeGroups,
            bool includeOffset)
        {
            if (tokens.Count == 0)
            {
                return tokens;
            }

            var first = tokens[0];
            if (includeGetFind
                && (first.Equals("get", StringComparison.OrdinalIgnoreCase)
                    || first.Equals("find", StringComparison.OrdinalIgnoreCase)))
            {
                return Prepend("message", tokens);
            }

            if (includeGroups && first.Equals("groups", StringComparison.OrdinalIgnoreCase))
            {
                return Prepend("consumer", tokens);
            }

            if (includeOffset && first.Equals("offset", StringComparison.OrdinalIgnoreCase))
            {
                return Prepend("consumer", tokens);
            }

            return tokens;
        }

        private static IReadOnlyList<string> Prepend(string value, IReadOnlyList<string> tokens)
        {
            var list = new List<string>(tokens.Count + 1) { value };
            list.AddRange(tokens);
            return list;
        }
    }
}
