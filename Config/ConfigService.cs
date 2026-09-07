using System;
using System.IO;
using IniParser;
using IniParser.Model;

namespace Kafka.Tool.Cli.Config
{
    public static class ConfigService
    {
        private const string ConfigFileName = "config";

        static ConfigService()
        {
            if (!File.Exists(GetConfigPath()))
            {
                Set(GetDefault());
            }
        }

        public static ConfigModel Get()
        {
            var parser = new FileIniDataParser();
            var data = parser.ReadFile(GetConfigPath());

            return new ConfigModel
            {
                BrokerHost = data["default"]["host"],
                TimeoutInMs = int.Parse(data["default"]["timeout"])
            };
        }

        public static void Set(ConfigModel config)
        {
            var data = new IniData();
            data["default"]["host"] = config.BrokerHost;
            data["default"]["timeout"] = config.TimeoutInMs.ToString();
            
            var parser = new FileIniDataParser();
            parser.WriteFile(GetConfigPath(), data);
        }

        private static string GetConfigPath()
        {
            var path = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".kafka", ConfigFileName);
            if (!Directory.Exists(Path.GetDirectoryName(path)))
            {
                Directory.CreateDirectory(Path.GetDirectoryName(path));
            }

            return path;            
        }

        private static ConfigModel GetDefault()
        {
            return new ConfigModel
            {
                BrokerHost = "localhost:9092",
                TimeoutInMs = 5000
            };
        }
    }
}