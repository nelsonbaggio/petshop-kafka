using System;
using Confluent.Kafka;

namespace Producer
{
  class Program
  {
    static void Main(string[] args)
    {
      var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
      var message = "1;42";
      using (var producer = new ProducerBuilder<Null, string>(config).Build())
      {
        {
          try
          {
            var sendResult = producer
                                .ProduceAsync("PETSHOP_NEW_ORDER", new Message<Null, string> { Value = message })
                                    .GetAwaiter()
                                        .GetResult();

            Console.WriteLine($"Mensagem '{sendResult.Value}' de '{sendResult.TopicPartitionOffset}'");
          }
          catch (ProduceException<Null, string> e)
          {
            Console.WriteLine($"Delivery failed: {e.Error.Reason}");
          }
        }
      }
    }
  }
}
