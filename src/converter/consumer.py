import pika
import sys
import os
import time
from pymongo import MongoClient
import gridfs
from convert import to_mp3


def main():
    # MongoDB Atlas URIs
    uri_video = "mongodb+srv://nileshpant112:MbnQSoDSsZw6gMRU@cluster0.8vagp.mongodb.net/videos?retryWrites=true&w=majority&appName=Cluster0"
    uri_mp3 = "mongodb+srv://nileshpant112:MbnQSoDSsZw6gMRU@cluster0.8vagp.mongodb.net/mp3s?retryWrites=true&w=majority&appName=Cluster0"

    # Create MongoClient instances for video and mp3 databases
    client_video = MongoClient(uri_video)
    client_mp3 = MongoClient(uri_mp3)

    # Access video and mp3 databases
    db_videos = client_video.videos
    db_mp3s = client_mp3.mp3s

    # Initialize GridFS for both video and mp3 databases
    fs_videos = gridfs.GridFS(db_videos)
    fs_mp3s = gridfs.GridFS(db_mp3s)

    # RabbitMQ connection
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel = connection.channel()

    def callback(ch, method, properties, body):
        err = to_mp3.start(body, fs_videos, fs_mp3s, ch)
        if err:
            ch.basic_nack(delivery_tag=method.delivery_tag)
        else:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    # Start consuming messages from the specified queue
    channel.basic_consume(
        queue=os.environ.get("VIDEO_QUEUE"), on_message_callback=callback
    )

    print("Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
