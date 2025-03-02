import os, gridfs, pika, json
from flask import Flask, request, send_file
from flask_pymongo import PyMongo
from auth import validate
from auth_svc import access
from storage import util
from bson.objectid import ObjectId

server = Flask(__name__)
server.debug = True

server.logger.info("This is an info log message")


uri_video = "mongodb+srv://nileshpant112:MbnQSoDSsZw6gMRU@cluster0.8vagp.mongodb.net/videos?retryWrites=true&w=majority&appName=Cluster0"
uri_mp3 = "mongodb+srv://nileshpant112:MbnQSoDSsZw6gMRU@cluster0.8vagp.mongodb.net/mp3s?retryWrites=true&w=majority&appName=Cluster0"

mongo_video = PyMongo(server, uri=uri_video)
mongo_mp3 = PyMongo(server, uri=uri_mp3)

fs_videos = gridfs.GridFS(mongo_video.db)
fs_mp3s = gridfs.GridFS(mongo_mp3.db)

connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
channel = connection.channel()


@server.route("/login", methods=["POST"])
def login():
    token, err = access.login(request)

    if not err:
        return token
    else:
        return err


@server.route("/upload", methods=["POST"])
def upload():
    access, err = validate.token(request)

    if err:
        return err

    access = json.loads(access)
    server.logger.debug('loaded access token', access['username'], access['admin'])
        
    if access["admin"]:
        if len(request.files) > 1 or len(request.files) < 1:
            return "exactly 1 file required", 400

        for _, f in request.files.items():
            server.logger.debug('Debug message inside upload', f, fs_videos)
            err = util.upload(f, fs_videos, channel, access)

            if err:
                return err

        return "success!", 200
    else:
        return "not authorized", 401

@server.route("/ready", methods=["GET"])
def ready():
    return 'ready', 200

@server.route("/download", methods=["GET"])
def download():
    access, err = validate.token(request)

    if err:
        return err

    access = json.loads(access)

    if access["admin"]:
        fid_string = request.args.get("fid")

        if not fid_string:
            return "fid is required", 400

        try:
            out = fs_mp3s.get(ObjectId(fid_string))
            return send_file(out, download_name=f"{fid_string}.mp3")
        except Exception as err:
            print(err)
            return "internal server error", 500

    return "not authorized", 401


if __name__ == "__main__":
    server.run(host="0.0.0.0", port=8080)
