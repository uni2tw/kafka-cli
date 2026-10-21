using System;
using McMaster.Extensions.CommandLineUtils;
using Kafka.Tool.Cli.Kafka;
using System.Text;

namespace Kafka.Tool.Cli
{
    class Program
    {
        public static void Main(string[] args) 
        {
            // 設定 Console 輸入與輸出的編碼
            Console.InputEncoding = Encoding.UTF8;
            Console.OutputEncoding = Encoding.UTF8;

            CommandLineApplication.Execute<KafkaCommand>(args);
            if (System.Diagnostics.Debugger.IsAttached)
            {
                Console.WriteLine("Press any key to stop.");
                Console.ReadKey();
            }
        }

    }
}
