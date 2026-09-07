using System.Collections.Generic;

namespace Kafka.Tool.Cli.Shell
{
    internal sealed class ReplContext
    {
        private readonly List<string> _history = new();

        public string DefaultTopic { get; private set; }
        public string DefaultGroup { get; private set; }

        public IReadOnlyList<string> History => _history;

        public void AddHistory(string input)
        {
            _history.Add(input);
        }

        public void SetTopic(string topic)
        {
            DefaultTopic = string.IsNullOrWhiteSpace(topic) ? null : topic.Trim();
        }

        public void SetGroup(string group)
        {
            DefaultGroup = string.IsNullOrWhiteSpace(group) ? null : group.Trim();
        }

        public void Clear()
        {
            DefaultTopic = null;
            DefaultGroup = null;
        }
    }
}
