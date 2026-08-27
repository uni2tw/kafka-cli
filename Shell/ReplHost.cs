using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Kafka.Tool.Cli.Config;
using Kafka.Tool.Cli.Kafka;
using McMaster.Extensions.CommandLineUtils;

namespace Kafka.Tool.Cli.Shell
{
    internal sealed class ReplHost
    {
        private readonly IConsole _console;
        private readonly ReplContext _context = new();
        private readonly ReplLineEditor _lineEditor;

        public ReplHost(IConsole console)
        {
            _console = console;
            _lineEditor = new ReplLineEditor(console, new ReplCompletion());
        }

        public async Task<int> RunAsync()
        {
            WriteBanner();
            KafkaClient.WarmAllConsumeGroupsCache();

            while (true)
            {
                string input = _lineEditor.ReadLine(GetPrompt(), _context);
                if (input == null)
                {
                    _console.WriteLine();
                    return 0;
                }

                input = input.Trim();
                if (input.Length == 0)
                {
                    continue;
                }

                _context.AddHistory(input);

                if (IsExitCommand(input))
                {
                    return 0;
                }

                if (IsHelpCommand(input))
                {
                    WriteHelp();
                    continue;
                }

                try
                {
                    if (await TryHandleContextCommandAsync(input))
                    {
                        continue;
                    }
                }
                catch (Exception ex)
                {
                    _console.Error.WriteLine(ex.Message);
                    continue;
                }

                if (IsHistoryCommand(input))
                {
                    WriteHistory();
                    continue;
                }

                if (IsClearCommand(input))
                {
                    Console.Clear();
                    continue;
                }

                try
                {
                    var args = ReplCommandLine.Tokenize(input);
                    if (args.Length == 0)
                    {
                        continue;
                    }

                    if (!ReplOutputRedirection.TryParse(args, out var commandArgs, out var outputTarget, out var redirectionError))
                    {
                        _console.Error.WriteLine(redirectionError);
                        continue;
                    }

                    if (outputTarget == null)
                    {
                        CommandLineApplication.Execute<KafkaCommand>(ApplyContext(commandArgs));
                    }
                    else
                    {
                        RunWithOutputRedirection(commandArgs, outputTarget);
                    }
                }
                catch (CommandParsingException ex)
                {
                    _console.Error.WriteLine(ex.Message);
                }
                catch (Exception ex)
                {
                    _console.Error.WriteLine(ex.Message);
                }
            }
        }

        private void WriteBanner()
        {
            _console.WriteLine("Kafka CLI Shell");
            _console.WriteLine("Type 'help' for shell help, or run any kafka-cli command without the 'kafka-cli' prefix.");
            _console.WriteLine("Use 'topic <name>' or 'use group <name>' to set shell defaults.");
            _console.WriteLine("Shortcuts: get, find, groups, and offset can be used without prefixes.");
            _console.WriteLine("Consumer-group cache warming in background. Topic-aware -g completion appears as cache becomes ready.");
            _console.WriteLine();
        }

        private string GetPrompt()
        {
            string brokerHost;
            try
            {
                brokerHost = ConfigService.Get().BrokerHost;
            }
            catch
            {
                brokerHost = "unknown-broker";
            }

            return $"kafka[{FormatBrokerHostForPrompt(brokerHost)}]> ";
        }

        private static string FormatBrokerHostForPrompt(string brokerHost)
        {
            if (string.IsNullOrWhiteSpace(brokerHost))
            {
                return "unknown-broker";
            }

            var hosts = brokerHost.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (hosts.Length <= 1)
            {
                return brokerHost.Trim();
            }

            return $"{hosts[0]} x{hosts.Length}";
        }

        private static bool IsExitCommand(string input)
        {
            return input.Equals("exit", StringComparison.OrdinalIgnoreCase)
                || input.Equals("quit", StringComparison.OrdinalIgnoreCase);
        }

        private static bool IsHelpCommand(string input)
        {
            return input.Equals("help", StringComparison.OrdinalIgnoreCase)
                || input.Equals("?", StringComparison.OrdinalIgnoreCase);
        }

        private static bool IsClearCommand(string input)
        {
            return input.Equals("clear", StringComparison.OrdinalIgnoreCase)
                || input.Equals("cls", StringComparison.OrdinalIgnoreCase);
        }

        private static bool IsHistoryCommand(string input)
        {
            return input.Equals("history", StringComparison.OrdinalIgnoreCase);
        }

        private async Task<bool> TryHandleContextCommandAsync(string input)
        {
            var args = ReplCommandLine.Tokenize(input);
            if (args.Length == 0)
            {
                return false;
            }

            if (args.Length == 1 && args[0].Equals("context", StringComparison.OrdinalIgnoreCase))
            {
                WriteContext();
                return true;
            }

            if (args.Length == 2
                && args[0].Equals("use", StringComparison.OrdinalIgnoreCase)
                && args[1].Equals("clear", StringComparison.OrdinalIgnoreCase))
            {
                _context.Clear();
                _console.WriteLine("Shell context cleared.");
                return true;
            }

            if (args.Length >= 3 && args[0].Equals("use", StringComparison.OrdinalIgnoreCase))
            {
                var value = string.Join(" ", args, 2, args.Length - 2);

                if (args[1].Equals("topic", StringComparison.OrdinalIgnoreCase))
                {
                    _console.WriteLine("'use topic X' has been removed. Use 'topic X' instead.");
                    return true;
                }

                if (args[1].Equals("group", StringComparison.OrdinalIgnoreCase))
                {
                    _context.SetGroup(value);
                    _console.WriteLine($"Default group set to '{_context.DefaultGroup}'.");
                    return true;
                }
            }

            if (args.Length == 2
                && args[0].Equals("topic", StringComparison.OrdinalIgnoreCase)
                && !IsOptionToken(args[1]))
            {
                _context.SetTopic(args[1]);
                KafkaClient.RefreshConsumeGroupsCache(_context.DefaultTopic);
                _console.WriteLine($"Default topic set to '{_context.DefaultTopic}'.");
                await OfferGroupSelectionAsync(_context.DefaultTopic);
                return true;
            }

            return false;
        }

        private static bool IsOptionToken(string token)
        {
            return token.StartsWith("-", StringComparison.Ordinal);
        }

        private async Task OfferGroupSelectionAsync(string topic)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                return;
            }

            IReadOnlyList<string> groups;
            if (KafkaClient.TryGetCachedConsumeGroups(topic, out var cachedGroups))
            {
                groups = cachedGroups;
            }
            else
            {
                groups = (await KafkaClient.GetConsumeGroupsAsync(topic)).ToList();
                KafkaClient.SetCachedConsumeGroups(topic, groups);
            }

            if (groups.Count == 0)
            {
                _console.WriteLine($"No handler(group) found for topic '{topic}'.");
                return;
            }

            _console.WriteLine($"Handlers(groups) for '{topic}':");
            for (int i = 0; i < groups.Count; i++)
            {
                _console.WriteLine($"  {i + 1}. {groups[i]}");
            }

            var selection = _lineEditor.ReadLine(
                "Select group number or name and press Enter (empty to skip): ",
                _context,
                (_, __) => GetGroupSelectionCandidates(groups));
            if (string.IsNullOrWhiteSpace(selection))
            {
                return;
            }

            if (TryResolveSelectedGroup(selection, groups, out var selectedGroup))
            {
                _context.SetGroup(selectedGroup);
                _console.WriteLine($"Default group set to '{_context.DefaultGroup}'.");
                return;
            }

            _console.WriteLine("Invalid selection. Group was not changed.");
        }

        private static IReadOnlyList<string> GetGroupSelectionCandidates(IReadOnlyList<string> groups)
        {
            var result = new List<string>(groups.Count * 2);
            for (int i = 0; i < groups.Count; i++)
            {
                result.Add((i + 1).ToString());
                result.Add(groups[i]);
            }

            return result;
        }

        private static bool TryResolveSelectedGroup(string selection, IReadOnlyList<string> groups, out string selectedGroup)
        {
            selectedGroup = null;

            if (int.TryParse(selection, out var selectedIndex)
                && selectedIndex >= 1
                && selectedIndex <= groups.Count)
            {
                selectedGroup = groups[selectedIndex - 1];
                return true;
            }

            selectedGroup = groups.FirstOrDefault(x => x.Equals(selection, StringComparison.OrdinalIgnoreCase));
            return !string.IsNullOrWhiteSpace(selectedGroup);
        }

        private string[] ApplyContext(string[] args)
        {
            if (args.Length == 0)
            {
                return args;
            }

            args = ReplCommandAliases.NormalizeForExecution(args).ToArray();

            if (NeedsTopicOption(args) && !HasOption(args, "-t", "--topic") && !string.IsNullOrWhiteSpace(_context.DefaultTopic))
            {
                args = AppendOption(args, "--topic", _context.DefaultTopic);
            }

            if (NeedsGroupOption(args) && !HasOption(args, "-g", "--group") && !string.IsNullOrWhiteSpace(_context.DefaultGroup))
            {
                args = AppendOption(args, "--group", _context.DefaultGroup);
            }

            return args;
        }

        private static bool NeedsTopicOption(string[] args)
        {
            return args.Length >= 2
                && ((args[0].Equals("message", StringComparison.OrdinalIgnoreCase)
                    && (args[1].Equals("get", StringComparison.OrdinalIgnoreCase)
                        || args[1].Equals("find", StringComparison.OrdinalIgnoreCase)
                        || args[1].Equals("find-many", StringComparison.OrdinalIgnoreCase)
                        || args[1].Equals("find-path", StringComparison.OrdinalIgnoreCase)
                        || args[1].Equals("consume", StringComparison.OrdinalIgnoreCase)
                        || args[1].Equals("clone", StringComparison.OrdinalIgnoreCase)
                        || args[1].Equals("remote-copy", StringComparison.OrdinalIgnoreCase)))
                    || (args[0].Equals("consumer", StringComparison.OrdinalIgnoreCase)
                        && args[1].Equals("offset", StringComparison.OrdinalIgnoreCase)));
        }

        private static bool NeedsGroupOption(string[] args)
        {
            return args.Length >= 2
                && ((args[0].Equals("message", StringComparison.OrdinalIgnoreCase)
                    && args[1].Equals("consume", StringComparison.OrdinalIgnoreCase))
                    || (args[0].Equals("consumer", StringComparison.OrdinalIgnoreCase)
                        && args[1].Equals("offset", StringComparison.OrdinalIgnoreCase)));
        }

        private static bool HasOption(string[] args, params string[] options)
        {
            foreach (var arg in args)
            {
                foreach (var option in options)
                {
                    if (arg.Equals(option, StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        private static string[] AppendOption(string[] args, string option, string value)
        {
            var result = new string[args.Length + 2];
            Array.Copy(args, result, args.Length);
            result[^2] = option;
            result[^1] = value;
            return result;
        }

        private void WriteHelp()
        {
            _console.WriteLine("Shell commands:");
            _console.WriteLine("  help         Show shell help");
            _console.WriteLine("  clear | cls  Clear the screen");
            _console.WriteLine("  history      Show command history");
            _console.WriteLine("  context      Show current shell defaults");
            _console.WriteLine("  topic        List all topics (optionally: topic -f filter)");
            _console.WriteLine("  topic X      Set default topic to X");
            _console.WriteLine("  use group X  Set default group");
            _console.WriteLine("  use clear    Clear shell defaults");
            _console.WriteLine("  exit | quit  Leave shell mode");
            _console.WriteLine();
            _console.WriteLine("Output redirection:");
            _console.WriteLine("  <command> > file.txt        Write output to file only (overwrite)");
            _console.WriteLine("  <command> >> file.txt       Write output to file only (append)");
            _console.WriteLine("  <command> | tee file.txt    Write output to screen and file");
            _console.WriteLine();
            _console.WriteLine("get usage (message get / get):");
            _console.WriteLine("  get -t MyTopic 100                 Get the message at offset 100");
            _console.WriteLine("  get -t MyTopic 100-110              Get offsets 100 through 110");
            _console.WriteLine("  get -t MyTopic -o -1                Get the newest message (negative index needs -o, not positional)");
            _console.WriteLine("  get -t MyTopic -o -100              Get the 100th message counting back from the newest");
            _console.WriteLine("  get -t MyTopic -p 0 -o -1           Same, but restricted to partition 0");
            _console.WriteLine();
            _console.WriteLine("find usage (message find / find):");
            _console.WriteLine("  find -t MyTopic 關鍵字            Keyword must be contained (implicit +)");
            _console.WriteLine("  find -t MyTopic *                 Match every message (no filter)");
            _console.WriteLine("  find -t MyTopic A-B                A must match, B must NOT match");
            _console.WriteLine("  find -t MyTopic A+B                A and B must both match");
            _console.WriteLine("  find -t MyTopic -n 50 A            Limit to first 50 matches");
            _console.WriteLine("  find -t MyTopic -s \"2026-08-01\" -e \"2026-08-05\" A   Time range filter");
            _console.WriteLine("  find -t MyTopic -so A               Append /*p{partition}:{offset}*/ to each match");
            _console.WriteLine("  find -t MyTopic -p data.field A     Extract a JSON field/path instead of the raw message");
            _console.WriteLine("  find -t MyTopic -n 50 A > result.txt   Combine with output redirection");
            _console.WriteLine();
            _console.WriteLine("Examples:");
            _console.WriteLine("  topic MyTopic   # then pick a group by number");
            _console.WriteLine("  get 12");
            _console.WriteLine("  groups -t MyTopic");
            _console.WriteLine("  offset -t MyTopic -g MyGroup -o 0");
            _console.WriteLine("  topic");
            _console.WriteLine("  topic -f MyFilter");
            _console.WriteLine("  message get -t MyTopic 12");
            _console.WriteLine("  message find -t MyTopic -n 50 > result.txt");
            _console.WriteLine("  consumer groups");
            _console.WriteLine("  consumer groups -t MyTopic | tee groups.txt");
            _console.WriteLine("  config");
            _console.WriteLine();
        }

        private void RunWithOutputRedirection(string[] commandArgs, ReplOutputTarget target)
        {
            var originalOut = Console.Out;
            var encoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);

            using var fileWriter = new StreamWriter(target.FilePath, append: target.Mode == ReplOutputMode.Append, encoding)
            {
                NewLine = "\r\n",
                AutoFlush = true
            };

            Console.SetOut(target.Mode == ReplOutputMode.Tee
                ? new TeeTextWriter(originalOut, fileWriter)
                : fileWriter);

            try
            {
                CommandLineApplication.Execute<KafkaCommand>(ApplyContext(commandArgs));
            }
            finally
            {
                Console.SetOut(originalOut);
            }
        }

        private void WriteContext()
        {
            string brokerHost;
            try
            {
                brokerHost = ConfigService.Get().BrokerHost;
            }
            catch
            {
                brokerHost = "unknown-broker";
            }

            _console.WriteLine("Shell context:");
            _console.WriteLine($"  broker: {brokerHost}");
            _console.WriteLine($"  topic: {_context.DefaultTopic ?? "(not set)"}");
            _console.WriteLine($"  group: {_context.DefaultGroup ?? "(not set)"}");
            _console.WriteLine();
        }

        private void WriteHistory()
        {
            for (int i = 0; i < _context.History.Count; i++)
            {
                _console.WriteLine($"{i + 1,3}: {_context.History[i]}");
            }

            _console.WriteLine();
        }
    }
}
