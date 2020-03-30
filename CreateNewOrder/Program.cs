using System;
using Confluent.Kafka;

namespace Producer
{
  class Program
  {
    static void Main(string[] args)
    {
      var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

      for (var i = 0; i < 10; i++)
      {
        var message = new Random().Next().ToString();
        var email = $"Processing new order: ['{message}']";
        var key = $"'{Guid.NewGuid().ToString()}' - '{message}'";
        using (var producer = new ProducerBuilder<string, string>(config).Build())
        {
          {
            try
            {
              var sendNewOrder = producer
                                  .ProduceAsync("PETSHOP_NEW_ORDER", new Message<string, string> { Key = key, Value = message })
                                      .GetAwaiter()
                                          .GetResult();

              Console.WriteLine($"New Order: '{sendNewOrder.Value}' ['{sendNewOrder.TopicPartitionOffset}']");

              var sendEmail = producer
                                  .ProduceAsync("PETSHOP_SEND_EMAIL", new Message<string, string> { Key = key, Value = email })
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
}
