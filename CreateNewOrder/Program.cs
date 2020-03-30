using System;
using Confluent.Kafka;

namespace Producer
{
  class Program
  {
    static void Main(string[] args)
    {
      var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

      var message = new Random().Next().ToString();
      var email = $"Processing new order: ['{message}']";

      using (var producer = new ProducerBuilder<Null, string>(config).Build())
      {
        {
          try
          {
            var sendNewOrder = producer
                                .ProduceAsync("PETSHOP_NEW_ORDER", new Message<Null, string> { Value = message })
                                    .GetAwaiter()
                                        .GetResult();

            Console.WriteLine($"New Order: '{sendNewOrder.Value}' ['{sendNewOrder.TopicPartitionOffset}']");

            var sendEmail = producer
                                .ProduceAsync("PETSHOP_SEND_EMAIL", new Message<Null, string> { Value = email })
                                    .GetAwaiter()
                                        .GetResult();

            Console.WriteLine($"Email: '{sendEmail.Value}' ['{sendEmail.TopicPartitionOffset}']");
          }
          catch (ProduceException<Null, string> e)
          {
            Console.WriteLine($"New Order failed: {e.Error.Reason}");
          }
        }
      }
    }
  }
}
