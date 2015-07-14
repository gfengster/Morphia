Morphia
MongoDB and Morphia example

DB cluster setup

cd $HOME_MONGDB

mkdir data1

mkdir data2

mkdir datta3

./bin/mongod --dbpath ./data1 --logpath ./log/log1  --port 28001 --replSet rs0 

./bin/mongod --dbpath ./data2 --logpath ./log/log2  --port 28002 --replSet rs0

./bin/mongod --dbpath ./data3 --logpath ./log/log3  --port 28003 --replSet rs0 

./bin/mongo --port 28001

>rs.initiate() 

>rs.conf()

>rs.status()

>rs.add("localhost:28002")

>rs.add("localhost:28003")

Run MorphiaApp
