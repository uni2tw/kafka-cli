using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Tool.Cli.Shell
{
    internal static class ReplCommandLine
    {
        public static string[] Tokenize(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
            {
                return Array.Empty<string>();
            }

            List<string> tokens = new();
            StringBuilder current = new();
            bool inQuotes = false;
            char quoteChar = '"';

            for (int i = 0; i < input.Length; i++)
            {
                char ch = input[i];

                if (inQuotes)
                {
                    if (ch == '\\' && i + 1 < input.Length && input[i + 1] == quoteChar)
                    {
                        current.Append(quoteChar);
                        i++;
                        continue;
                    }

                    if (ch == quoteChar)
                    {
                        inQuotes = false;
                        continue;
                    }

                    current.Append(ch);
                    continue;
                }

                if (char.IsWhiteSpace(ch))
                {
                    FlushToken(tokens, current);
                    continue;
                }

                if (ch == '"' || ch == '\'')
                {
                    inQuotes = true;
                    quoteChar = ch;
                    continue;
                }

                if (TryConsumeOperator(input, ref i, tokens, current))
                {
                    continue;
                }

                current.Append(ch);
            }

            if (inQuotes)
            {
                throw new ArgumentException("Unterminated quoted string.");
            }

            FlushToken(tokens, current);
            return tokens.ToArray();
        }

        public static List<string> TokenizePartial(string input)
        {
            List<string> tokens = new();
            if (string.IsNullOrEmpty(input))
            {
                return tokens;
            }

            StringBuilder current = new();
            bool inQuotes = false;
            char quoteChar = '"';

            for (int i = 0; i < input.Length; i++)
            {
                char ch = input[i];

                if (inQuotes)
                {
                    if (ch == '\\' && i + 1 < input.Length && input[i + 1] == quoteChar)
                    {
                        current.Append(quoteChar);
                        i++;
                        continue;
                    }

                    if (ch == quoteChar)
                    {
                        inQuotes = false;
                        continue;
                    }

                    current.Append(ch);
                    continue;
                }

                if (char.IsWhiteSpace(ch))
                {
                    FlushToken(tokens, current);
                    continue;
                }

                if (ch == '"' || ch == '\'')
                {
                    inQuotes = true;
                    quoteChar = ch;
                    continue;
                }

                if (TryConsumeOperator(input, ref i, tokens, current))
                {
                    continue;
                }

                current.Append(ch);
            }

            FlushToken(tokens, current);
            return tokens;
        }

        private static void FlushToken(List<string> tokens, StringBuilder current)
        {
            if (current.Length == 0)
            {
                return;
            }

            tokens.Add(current.ToString());
            current.Clear();
        }

        /// <summary>
        /// Unquoted '>', '>>' and '|' always act as standalone redirection-operator tokens,
        /// even when glued to adjacent characters (e.g. "cmd>1.txt"), matching shell conventions.
        /// </summary>
        private static bool TryConsumeOperator(string input, ref int i, List<string> tokens, StringBuilder current)
        {
            char ch = input[i];
            if (ch != '>' && ch != '|')
            {
                return false;
            }

            FlushToken(tokens, current);

            if (ch == '>' && i + 1 < input.Length && input[i + 1] == '>')
            {
                tokens.Add(">>");
                i++;
            }
            else
            {
                tokens.Add(ch.ToString());
            }

            return true;
        }
    }
}
