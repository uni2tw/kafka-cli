using System;

namespace Kafka.Tool.Cli.Shell
{
    internal enum ReplOutputMode
    {
        Overwrite,
        Append,
        Tee
    }

    internal sealed class ReplOutputTarget
    {
        public ReplOutputTarget(string filePath, ReplOutputMode mode)
        {
            FilePath = filePath;
            Mode = mode;
        }

        public string FilePath { get; }
        public ReplOutputMode Mode { get; }
    }

    internal static class ReplOutputRedirection
    {
        public static bool TryParse(string[] tokens, out string[] commandArgs, out ReplOutputTarget target, out string error)
        {
            commandArgs = tokens;
            target = null;
            error = null;

            if (tokens.Length >= 4
                && tokens[^3].Equals("|", StringComparison.Ordinal)
                && tokens[^2].Equals("tee", StringComparison.OrdinalIgnoreCase))
            {
                commandArgs = tokens[..^3];
                if (commandArgs.Length == 0)
                {
                    error = "缺少要執行的指令。";
                    return false;
                }

                target = new ReplOutputTarget(tokens[^1], ReplOutputMode.Tee);
                return true;
            }

            if (tokens.Length >= 3
                && (tokens[^2].Equals(">", StringComparison.Ordinal) || tokens[^2].Equals(">>", StringComparison.Ordinal)))
            {
                commandArgs = tokens[..^2];
                if (commandArgs.Length == 0)
                {
                    error = "缺少要執行的指令。";
                    return false;
                }

                var mode = tokens[^2].Equals(">>", StringComparison.Ordinal) ? ReplOutputMode.Append : ReplOutputMode.Overwrite;
                target = new ReplOutputTarget(tokens[^1], mode);
                return true;
            }

            if (tokens.Length > 0 && IsRedirectionOperator(tokens[^1]))
            {
                error = tokens[^1].Equals("|", StringComparison.Ordinal)
                    ? "缺少 'tee <檔案>'，例如：... | tee result.txt"
                    : $"缺少要寫入的檔名，例如：... {tokens[^1]} result.txt";
                return false;
            }

            if (tokens.Length >= 2
                && tokens[^2].Equals("|", StringComparison.Ordinal)
                && !tokens[^1].Equals("tee", StringComparison.OrdinalIgnoreCase))
            {
                error = "目前只支援 '| tee <檔案>' 這種寫法。";
                return false;
            }

            return true;
        }

        private static bool IsRedirectionOperator(string token)
        {
            return token.Equals(">", StringComparison.Ordinal)
                || token.Equals(">>", StringComparison.Ordinal)
                || token.Equals("|", StringComparison.Ordinal);
        }
    }
}
