using System;
using System.Diagnostics;

namespace Kafka.Tool.Cli.Kafka
{
    // One status line rewritten in place on stderr; invisible when stderr is redirected, and removed by Clear().
    internal sealed class ScanProgress
    {
        private const int ShowAfterMs = 700;
        private const int RedrawEveryMs = 200;

        private readonly bool _enabled;
        private readonly Stopwatch _clock = Stopwatch.StartNew();
        private long _lastDrawMs = -RedrawEveryMs;
        private int _drawnLength;

        public ScanProgress(bool enabled)
        {
            _enabled = enabled && !Console.IsErrorRedirected;
        }

        public bool IsDue()
        {
            if (!_enabled)
            {
                return false;
            }

            var elapsed = _clock.ElapsedMilliseconds;
            return elapsed >= ShowAfterMs && elapsed - _lastDrawMs >= RedrawEveryMs;
        }

        public void Draw(long scanned, long matches, long total)
        {
            _lastDrawMs = _clock.ElapsedMilliseconds;

            var text = total > 0
                ? $"scanned {scanned:N0} / {total:N0} ({Math.Min(100, scanned * 100 / total)}%) | matches {matches:N0} | {_clock.Elapsed.TotalSeconds:N1}s"
                : $"scanned {scanned:N0} | matches {matches:N0} | {_clock.Elapsed.TotalSeconds:N1}s";

            // a line that wraps can no longer be overwritten with \r
            int width;
            try
            {
                width = Console.WindowWidth;
            }
            catch
            {
                width = 80;
            }

            if (width > 1 && text.Length > width - 1)
            {
                text = text.Substring(0, width - 1);
            }

            Console.Error.Write("\r" + text.PadRight(_drawnLength));
            _drawnLength = text.Length;
        }

        public void Clear()
        {
            if (_drawnLength == 0)
            {
                return;
            }

            Console.Error.Write("\r" + new string(' ', _drawnLength) + "\r");
            _drawnLength = 0;
        }
    }
}
