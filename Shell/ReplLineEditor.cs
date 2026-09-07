using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kafka.Tool.Cli.Kafka;
using McMaster.Extensions.CommandLineUtils;

namespace Kafka.Tool.Cli.Shell
{
    internal sealed class ReplLineEditor
    {
        private readonly IConsole _console;
        private readonly ReplCompletion _completion;

        public ReplLineEditor(IConsole console, ReplCompletion completion)
        {
            _console = console;
            _completion = completion;
        }

        public string ReadLine(string prompt, ReplContext context)
        {
            return ReadLine(prompt, context, null);
        }

        public string ReadLine(
            string prompt,
            ReplContext context,
            Func<string, ReplContext, IReadOnlyList<string>> completionProvider)
        {
            _console.Write(prompt);

            var buffer = new StringBuilder();
            int cursor = 0;
            int historyIndex = context.History.Count;
            string historyDraft = string.Empty;
            string lastWarmedTopic = null;
            int startLeft = Console.CursorLeft;
            int startTop = Console.CursorTop;
            int renderedLineCount = 1;

            while (true)
            {
                var key = Console.ReadKey(intercept: true);

                if (key.Key == ConsoleKey.Enter)
                {
                    _console.WriteLine();
                    return buffer.ToString();
                }

                if (key.Key == ConsoleKey.Backspace)
                {
                    if (cursor > 0)
                    {
                        buffer.Remove(cursor - 1, 1);
                        cursor--;
                        WarmTopicGroupCache(buffer.ToString(), context, ref lastWarmedTopic);
                        Redraw(buffer, cursor, startLeft, startTop, ref renderedLineCount);
                    }
                    continue;
                }

                if (key.Key == ConsoleKey.Delete)
                {
                    if (cursor < buffer.Length)
                    {
                        buffer.Remove(cursor, 1);
                        WarmTopicGroupCache(buffer.ToString(), context, ref lastWarmedTopic);
                        Redraw(buffer, cursor, startLeft, startTop, ref renderedLineCount);
                    }
                    continue;
                }

                if (key.Key == ConsoleKey.LeftArrow)
                {
                    if (cursor > 0)
                    {
                        cursor--;
                        Redraw(buffer, cursor, startLeft, startTop, ref renderedLineCount);
                    }
                    continue;
                }

                if (key.Key == ConsoleKey.RightArrow)
                {
                    if (cursor < buffer.Length)
                    {
                        cursor++;
                        Redraw(buffer, cursor, startLeft, startTop, ref renderedLineCount);
                    }
                    continue;
                }

                if (key.Key == ConsoleKey.Home)
                {
                    cursor = 0;
                    Redraw(buffer, cursor, startLeft, startTop, ref renderedLineCount);
                    continue;
                }

                if (key.Key == ConsoleKey.End)
                {
                    cursor = buffer.Length;
                    Redraw(buffer, cursor, startLeft, startTop, ref renderedLineCount);
                    continue;
                }

                if (key.Key == ConsoleKey.UpArrow)
                {
                    if (context.History.Count == 0)
                    {
                        continue;
                    }

                    if (historyIndex == context.History.Count)
                    {
                        historyDraft = buffer.ToString();
                    }

                    if (historyIndex > 0)
                    {
                        historyIndex--;
                        ReplaceBuffer(buffer, context.History[historyIndex], ref cursor, startLeft, startTop, ref renderedLineCount);
                        WarmTopicGroupCache(buffer.ToString(), context, ref lastWarmedTopic);
                    }
                    continue;
                }

                if (key.Key == ConsoleKey.DownArrow)
                {
                    if (context.History.Count == 0)
                    {
                        continue;
                    }

                    if (historyIndex < context.History.Count - 1)
                    {
                        historyIndex++;
                        ReplaceBuffer(buffer, context.History[historyIndex], ref cursor, startLeft, startTop, ref renderedLineCount);
                        WarmTopicGroupCache(buffer.ToString(), context, ref lastWarmedTopic);
                    }
                    else if (historyIndex == context.History.Count - 1)
                    {
                        historyIndex = context.History.Count;
                        ReplaceBuffer(buffer, historyDraft, ref cursor, startLeft, startTop, ref renderedLineCount);
                        WarmTopicGroupCache(buffer.ToString(), context, ref lastWarmedTopic);
                    }
                    continue;
                }

                if (key.Key == ConsoleKey.Tab)
                {
                    HandleCompletion(
                        prompt,
                        buffer,
                        ref cursor,
                        ref startLeft,
                        ref startTop,
                        ref renderedLineCount,
                        context,
                        completionProvider);
                    WarmTopicGroupCache(buffer.ToString(), context, ref lastWarmedTopic);
                    historyIndex = context.History.Count;
                    historyDraft = buffer.ToString();
                    continue;
                }

                if (!char.IsControl(key.KeyChar))
                {
                    buffer.Insert(cursor, key.KeyChar);
                    cursor++;
                    WarmTopicGroupCache(buffer.ToString(), context, ref lastWarmedTopic);
                    Redraw(buffer, cursor, startLeft, startTop, ref renderedLineCount);
                }
            }
        }

        private static void WarmTopicGroupCache(string input, ReplContext context, ref string lastWarmedTopic)
        {
            var tokens = ReplCommandLine.TokenizePartial(input);
            var topic = TryGetOptionValue(tokens, "-t", "--topic");
            if (string.IsNullOrWhiteSpace(topic))
            {
                topic = context.DefaultTopic;
            }

            if (string.IsNullOrWhiteSpace(topic))
            {
                lastWarmedTopic = null;
                return;
            }

            topic = topic.Trim();
            if (!topic.Equals(lastWarmedTopic, StringComparison.OrdinalIgnoreCase))
            {
                KafkaClient.RefreshConsumeGroupsCache(topic);
                lastWarmedTopic = topic;
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

        private void HandleCompletion(
            string prompt,
            StringBuilder buffer,
            ref int cursor,
            ref int startLeft,
            ref int startTop,
            ref int renderedLineCount,
            ReplContext context,
            Func<string, ReplContext, IReadOnlyList<string>> completionProvider)
        {
            string input = buffer.ToString();
            var candidates = completionProvider != null
                ? completionProvider(input, context)
                : _completion.GetCandidates(input, context);
            if (candidates.Count == 0)
            {
                return;
            }

            var tokens = ReplCommandLine.TokenizePartial(input);
            bool trailingSpace = input.Length > 0 && char.IsWhiteSpace(input[^1]);
            string currentToken = trailingSpace ? string.Empty : tokens.LastOrDefault() ?? string.Empty;

            if (candidates.Count == 1)
            {
                ApplyCompletion(buffer, ref cursor, currentToken, candidates[0], trailingSpace, startLeft, startTop, ref renderedLineCount);
                return;
            }

            // Still more than one candidate after this Tab press: extend as far as the shared
            // prefix allows (if any), then immediately show the remaining options below instead
            // of waiting for a second Tab — otherwise the extended text alone reads as "done".
            string commonPrefix = GetCommonPrefix(candidates);
            if (!string.IsNullOrEmpty(commonPrefix)
                && commonPrefix.Length > currentToken.Length)
            {
                ApplyCompletion(buffer, ref cursor, currentToken, commonPrefix, trailingSpace, startLeft, startTop, ref renderedLineCount, appendSpace: false);
            }

            _console.WriteLine();
            foreach (var candidate in candidates)
            {
                _console.WriteLine(candidate);
            }

            startLeft = 0;
            startTop = Console.CursorTop;
            renderedLineCount = 1;
            _console.Write(prompt);
            startLeft = Console.CursorLeft;
            startTop = Console.CursorTop;
            Redraw(buffer, cursor, startLeft, startTop, ref renderedLineCount);
        }

        private static string GetCommonPrefix(IReadOnlyList<string> values)
        {
            if (values.Count == 0)
            {
                return string.Empty;
            }

            string prefix = values[0];
            foreach (var value in values.Skip(1))
            {
                int len = 0;
                while (len < prefix.Length && len < value.Length
                    && char.ToLowerInvariant(prefix[len]) == char.ToLowerInvariant(value[len]))
                {
                    len++;
                }

                prefix = prefix[..len];
                if (prefix.Length == 0)
                {
                    break;
                }
            }

            return prefix;
        }

        private static void ApplyCompletion(
            StringBuilder buffer,
            ref int cursor,
            string currentToken,
            string candidate,
            bool trailingSpace,
            int startLeft,
            int startTop,
            ref int renderedLineCount,
            bool appendSpace = true)
        {
            if (!trailingSpace && currentToken.Length > 0)
            {
                buffer.Remove(cursor - currentToken.Length, currentToken.Length);
                cursor -= currentToken.Length;
            }

            buffer.Insert(cursor, candidate);
            cursor += candidate.Length;
            if (appendSpace)
            {
                buffer.Insert(cursor, ' ');
                cursor++;
            }

            Redraw(buffer, cursor, startLeft, startTop, ref renderedLineCount);
        }

        private static void ReplaceBuffer(
            StringBuilder buffer,
            string value,
            ref int cursor,
            int startLeft,
            int startTop,
            ref int renderedLineCount)
        {
            buffer.Clear();
            buffer.Append(value);
            cursor = buffer.Length;
            Redraw(buffer, cursor, startLeft, startTop, ref renderedLineCount);
        }

        private static void Redraw(StringBuilder buffer, int cursor, int startLeft, int startTop, ref int renderedLineCount)
        {
            int bufferWidth = GetSafeBufferWidth();
            int availableWidth = Math.Max(1, bufferWidth - startLeft);
            string content = buffer.ToString();
            int visibleStart = GetVisibleStart(cursor, content.Length, availableWidth);
            bool hasHiddenLeft = visibleStart > 0;
            int contentWidth = Math.Max(0, availableWidth - (hasHiddenLeft ? 1 : 0));
            int visibleLength = Math.Min(contentWidth, Math.Max(0, content.Length - visibleStart));
            string visibleContent = visibleLength > 0
                ? content.Substring(visibleStart, visibleLength)
                : string.Empty;
            if (hasHiddenLeft)
            {
                visibleContent = "<" + visibleContent;
            }

            Console.SetCursorPosition(startLeft, startTop);
            Console.Write(new string(' ', Math.Max(1, bufferWidth - startLeft)));
            Console.SetCursorPosition(startLeft, startTop);
            Console.Write(visibleContent);
            renderedLineCount = 1;
            PositionCursor(cursor, startLeft, startTop, visibleStart, hasHiddenLeft);
        }

        private static void PositionCursor(int cursor, int startLeft, int startTop)
        {
            int bufferWidth = GetSafeBufferWidth();
            int availableWidth = Math.Max(1, bufferWidth - startLeft);
            int visibleStart = GetVisibleStart(cursor, cursor, availableWidth);
            PositionCursor(cursor, startLeft, startTop, visibleStart, visibleStart > 0);
        }

        private static void PositionCursor(int cursor, int startLeft, int startTop, int visibleStart, bool hasHiddenLeft)
        {
            int bufferWidth = Math.Max(Console.BufferWidth, 1);
            int bufferHeight = Math.Max(Console.BufferHeight, 1);
            int left = startLeft + (hasHiddenLeft ? 1 : 0) + Math.Max(0, cursor - visibleStart);
            left = Math.Max(0, Math.Min(left, bufferWidth - 1));
            startTop = Math.Max(0, Math.Min(startTop, bufferHeight - 1));
            Console.SetCursorPosition(left, startTop);
        }

        private static int GetSafeBufferWidth()
        {
            return Math.Max(Console.BufferWidth, 1);
        }

        private static int GetVisibleStart(int cursor, int contentLength, int availableWidth)
        {
            if (contentLength <= availableWidth)
            {
                return 0;
            }

            if (cursor < availableWidth)
            {
                return 0;
            }

            return cursor - availableWidth + 1;
        }
    }
}
