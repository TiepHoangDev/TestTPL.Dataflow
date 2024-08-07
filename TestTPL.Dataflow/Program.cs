using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace TestTPL.Dataflow
{
    internal class Program
    {
        static void Main(string[] args)
        {
            try
            {
                Demo().Wait();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

            Console.WriteLine("Done");
            Console.ReadKey();
        }

        private static async Task Demo()
        {
            var demo = new Demo();
            var opt = new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 10,
            };
            var dfo = new DataflowLinkOptions
            {
                PropagateCompletion = true,
                Append = true,
                MaxMessages = -1,
            };

            var blockStart = new TransformManyBlock<int, SettingItem>(async number => await demo.ReadSetting(), opt);
            var blockProcess = new TransformBlock<SettingItem, SettingItem>(demo.Process, opt);
            var blockSaveToSQL = new ActionBlock<SettingItem>(demo.SaveToSQL, opt);

            blockStart.LinkTo(blockProcess, dfo);
            blockProcess.LinkTo(blockSaveToSQL, dfo);


            for (int i = 0; i < 3; i++)
            {
                blockStart.Post(i);
                await Task.Delay(100);
            }

            blockStart.Complete();
            await blockSaveToSQL.Completion;
        }
    }

    public class Demo
    {
        public async Task<List<SettingItem>> ReadSetting()
        {
            return new List<SettingItem> {
                new SettingItem{ Url = "Url-1", Enable = true, Delay = TimeSpan.FromSeconds(1)  },
                new SettingItem{ Url = "Url-2", Enable = true, Delay = TimeSpan.FromSeconds(2)  },
                new SettingItem{ Url = "Url-3", Enable = true, Delay = TimeSpan.FromSeconds(3)  },
            };
        }

        public async Task<SettingItem> Process(SettingItem item)
        {
            await Task.Delay(item.Delay);
            await Console.Out.WriteLineAsync($"->Process {item.Url} done.");
            throw new Exception("Test err Process");
            return item;
        }

        public async Task SaveToSQL(SettingItem item)
        {
            await Console.Out.WriteLineAsync($"\t->SaveToSQL {item.Url} done.");
        }
    }

    public class SettingItem
    {
        public string Url { get; set; }
        public bool Enable { get; set; }
        public TimeSpan Delay { get; set; }
    }
}
