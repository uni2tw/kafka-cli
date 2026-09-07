using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;

namespace Kafka.Tool.Cli.Shell
{
    [Command("shell", Description = "Start interactive shell mode")]
    public class ShellCommand
    {
        private async Task<int> OnExecute(IConsole console)
        {
            var host = new ReplHost(console);
            return await host.RunAsync();
        }
    }
}
