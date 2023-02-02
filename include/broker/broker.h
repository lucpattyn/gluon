#include <iostream>
#include <fstream>
#include <map>
#include <vector>
#include <string>
#include <zmq.hpp>

const int buffer_size = 100; // size of in-memory buffer

struct Message
{
    std::string topic;
    std::string payload;
};

#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>

class MessageStore {
 public:
  MessageStore(size_t buffer_size) : buffer_size_(buffer_size), head_(0), tail_(0) {}

  void AddMessage(const std::string& message) {
    // Save the message to the buffer
    buffer_[head_] = message;
    head_ = (head_ + 1) % buffer_size_;
    if (head_ == tail_)
      tail_ = (tail_ + 1) % buffer_size_;

    // Open the head-tail indices file in write mode
    std::ofstream head_tail_file("head_tail.bin", std::ios::binary);
    head_tail_file.write((char*)&head_, sizeof(head_));
    head_tail_file.write((char*)&tail_, sizeof(tail_));
    head_tail_file.close();

    // Open the message store file in write mode and append the message
    std::ofstream file("message_store.bin", std::ios::app | std::ios::binary);
    size_t message_size = message.size();
    file.write((char*)&message_size, sizeof(message_size));
    file.write(message.c_str(), message.size());
    file.close();
  }

  std::vector<std::string> GetMessages() {
    std::vector<std::string> messages;

    // Open the head-tail indices file in read mode
    std::ifstream head_tail_file("head_tail.bin", std::ios::binary);
    head_tail_file.read((char*)&head_, sizeof(head_));
    head_tail_file.read((char*)&tail_, sizeof(tail_));
    head_tail_file.close();

    // Open the message store file in read mode
    std::ifstream file("message_store.bin", std::ios::binary);
    while (tail_ != head_) {
      size_t message_size;
      file.read((char*)&message_size, sizeof(message_size));
      std::vector<char> message_buffer(message_size);
      file.read(message_buffer.data(), message_size);
      messages.push_back(std::string(message_buffer.begin(), message_buffer.end()));
      tail_ = (tail_ + 1) % buffer_size_;
    }
    file.close();

    return messages;
  }

 private:
  size_t buffer_size_;
  std::string buffer_[100];
  size_t head_;
  size_t tail_;
};


void MessageBroker::process_message(const Message& message)
{
    int confirmations_received = 0;
    int retry_attempts = 0;
    int timeout_ms = 1000;

    while (confirmations_received < subscribers.size() && retry_attempts < MAX_RETRY_ATTEMPTS)
    {
        // Send the message to each subscriber
        for (const auto& [identity, subscriber] : subscribers)
        {
            subscriber->send(message.data);
        }

        // Wait for confirmations from subscribers
        zmq::pollitem_t items[subscribers.size()];
        int item_index = 0;
        for (const auto& [identity, subscriber] : subscribers)
        {
            items[item_index].socket = *subscriber;
            items[item_index].events = ZMQ_POLLIN;
            item_index++;
        }

        int poll_result = zmq::poll(items, subscribers.size(), timeout_ms);
        if (poll_result > 0)
        {
            for (int i = 0; i < subscribers.size(); i++)
            {
                if (items[i].revents & ZMQ_POLLIN)
                {
                    zmq::message_t confirmation;
                    subscribers[i]->recv(&confirmation);
                    confirmations_received++;
                }
            }
        }
        else
        {
            retry_attempts++;
        }
    }

}


void BrokerThread(zmq::context_t& context)
{
    zmq::socket_t broker(context, ZMQ_ROUTER);
    broker.bind("tcp://*:5557");

    MessageStore store("message_store.bin");

    while (true)
    {
        // receive message from client
        zmq::message_t identity;
        broker.recv(&identity);

        zmq::message_t request;
        broker.recv(&request);
        std::string request_str(static_cast<char*>(request.data()), request.size());

        // split the request into topic and payload
        auto topic_end = request_str.find(':');
        std::string topic = request_str.substr(0, topic_end);
        std::string payload = request_str.substr(topic_end + 1);

        // add the message to the store
        store.AddMessage({ topic, payload });

        // send reply to client
        zmq::message_t reply(3);
        memcpy(reply.data(), "ACK", 3);
        broker.send(identity, ZMQ_SNDMORE);
        broker.send(reply);

        // send messages to subscribers
        std::map<std::string, zmq::socket_t*> subscribers;
        for (;;)
        {
            zmq::pollitem_t items[] = {
                { broker, 0, ZMQ_POLLIN, 0 },
            };
            zmq::poll(items, 1, 10);

            if (items[0].revents & ZMQ_POLLIN)
            {
                zmq::message_t identity;
                broker.recv(&identity);

                zmq::message_t request;
                broker.recv(&request);
                std::string request_str(static_cast<char*>(request.data()), request.size());

                // add subscriber
                if (request_str == "SUBSCRIBE")
                {
                    std::string identity_str(static_cast<char*>(identity.data()), identity.size());
                    auto iter = subscribers.find(identity_str);
                    if (iter == subscribers.end())
                    {
                        zmq::socket_t* subscriber = new zmq::socket_t(context, ZMQ_DEALER);
                        subscriber->connect("tcp://localhost:5557");
                        subscribers[identity_str] = subscriber;
                    }
                }
            }

            // send messages to subscribers
            for (const auto& message : store.GetMessages(topic))
            {
                process_message(message);
            }
        }
    }
}

int main()
{
    zmq::context_t context(1);
    std::thread broker_thread(BrokerThread, std::ref(context));
    broker_thread.join();

    return 0;
}


/*
#include <iostream>
#include <zmq.hpp>

int main()
{
    // Connect to the message broker
    zmq::context_t context;
    zmq::socket_t subscriber(context, ZMQ_SUB);
    subscriber.connect("tcp://localhost:5555");

    // Subscribe to all messages
    subscriber.setsockopt(ZMQ_SUBSCRIBE, "", 0);

    while (true)
    {
        // Receive a message
        zmq::message_t message;
        subscriber.recv(&message);

        // Process the message
        std::string message_str(static_cast<char*>(message.data()), message.size());
        std::cout << "Received message: " << message_str << std::endl;

        // Send a confirmation to the broker
        zmq::socket_t confirmation_sender(context, ZMQ_PUSH);
        confirmation_sender.connect("tcp://localhost:5556");
        zmq::message_t confirmation(5);
        memcpy(confirmation.data(), "ACK", 5);
        confirmation_sender.send(confirmation);
    }

    return 0;
}
*/
