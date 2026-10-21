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
                    || first.Equals("find", StringComparison.OrdinalIgnoreCase)
                    || first.Equals("produce", StringComparison.OrdinalIgnoreCase)
                    || first.Equals("resend", StringComparison.OrdinalIgnoreCase)))
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

            // skip N = offset -ofc N, goto N = offset -o N
            if (includeOffset && first.Equals("skip", StringComparison.OrdinalIgnoreCase))
            {
                return ReplaceFirst(tokens, "consumer", "offset", "-ofc");
            }

            if (includeOffset && first.Equals("goto", StringComparison.OrdinalIgnoreCase))
            {
                return ReplaceFirst(tokens, "consumer", "offset", "-o");
            }

            return tokens;
        }

        private static IReadOnlyList<string> ReplaceFirst(IReadOnlyList<string> tokens, params string[] replacement)
        {
            var list = new List<string>(replacement);
            for (int i = 1; i < tokens.Count; i++)
            {
                list.Add(tokens[i]);
            }

            return list;
        }

        private static IReadOnlyList<string> Prepend(string value, IReadOnlyList<string> tokens)
        {
            var list = new List<string>(tokens.Count + 1) { value };
            list.AddRange(tokens);
            return list;
        }
    }
}
