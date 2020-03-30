using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;

namespace LogProcessor
{
  class Program
  {
    static void Main(string[] args)
    {
      var config = new ConsumerConfig
      {
        GroupId = "new-pet-group-5",
        BootstrapServers = "localhost:9092",
        AutoOffsetReset = AutoOffsetReset.Earliest
      };

      using (var consumer = new ConsumerBuilder<string, string>(config).Build())
      {
        IEnumerable<string> topics = new string[]
        {
            "PETSHOP_NEW_ORDER",
            "PETSHOP_SEND_EMAIL"
        };

        consumer.Subscribe(topics);
        var cts = new CancellationTokenSource();

        try
        {
          while (true)
          {
            var message = consumer.Consume(cts.Token);
            Console.WriteLine("------------------------------------------------");
            Console.WriteLine($"Log: [{message.Topic}] {message.Value} [{message.TopicPartitionOffset}]");
          }
        }
        catch (OperationCanceledException)
        {
          consumer.Close();
        }
      }
    }
  }
}
