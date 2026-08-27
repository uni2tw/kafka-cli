using System;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;

namespace Kafka.Tool.Cli.Config
{
    [Command("config", Description = "Configure Kafka CLI")]
    public class ConfigCommand
    {        
        private async Task<int> OnExecute(IConsole console)
        {
            var currentConfig = ConfigService.Get();
            console.WriteLine("Current configuration:");
            console.WriteLine();
            console.WriteLine($"  Broker host is {currentConfig.BrokerHost}");
            console.WriteLine($"  Timeout is {currentConfig.TimeoutInMs} ms");
            console.WriteLine();

            if (!Prompt.GetYesNo("Do you want to update configuration?", false))
            {
                return 1;
            }

            var newConfig = new ConfigModel
            {
                BrokerHost = Prompt.GetString("Broker host:"),
                TimeoutInMs = Prompt.GetInt("Timeout in ms:")
            };
            console.WriteLine();

            ConfigService.Set(newConfig);
            console.WriteLine($"Config was saved.");

            return await Task.FromResult(1);
        }
    }
}