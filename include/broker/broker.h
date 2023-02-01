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

class MessageStore
{
public:
    MessageStore(const std::string& file_name)
        : file_name_(file_name)
    {
        // load messages from disk
        std::ifstream file(file_name, std::ios::binary);
        while (file)
        {
            Message message;
            file.read(reinterpret_cast<char*>(&message), sizeof(message));
            if (file)
                messages_.push_back(message);
        }
    }

    void AddMessage(const Message& message)
    {
        messages_.push_back(message);
        if (messages_.size() > buffer_size)
            Flush();
    }

    std::vector<Message> GetMessages(const std::string& topic)
    {
        std::vector<Message> result;
        for (const auto& message : messages_)
        {
            if (message.topic == topic)
                result.push_back(message);
        }
        return result;
    }

private:
    void Flush()
    {
        std::ofstream file(file_name_, std::ios::binary | std::ios::app);
        for (const auto& message : messages_)
        {
            file.write(reinterpret_cast<const char*>(&message), sizeof(message));
        }
        file.close();
        messages_.clear();
    }

    std::string file_name_;
    std::vector<Message> messages_;
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

    if (confirmations_received == subscribers.size())
    {
        // All confirmations have been received, so remove the message from the store
        message_store.remove_message(message.id);
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

