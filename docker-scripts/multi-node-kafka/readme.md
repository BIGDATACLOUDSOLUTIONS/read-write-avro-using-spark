**References**

<a href="https://codersee.com/how-to-deploy-multiple-kafka-brokers-with-docker-compose/" target="new">Install Kafka using Docker Compose</a>
<br><a href="https://betterprogramming.pub/adding-schema-registry-to-kafka-in-your-local-docker-environment-49ada28c8a9b" target="new">schema registery</a>

**Steps to Install 3 node kafka on docker**
1. Start the docker services: ```docker compose up```
2. Verify All Containers Running: ```docker ps```
3. Verify Active Kafka Brokers Using zookeeper-shell:
   - Run a zookeeper-shell on the zookeeper container:
     <br>```docker exec -it zookeeper /bin/zookeeper-shell localhost:2181```
   - Output Should look like:
    ```   
    Connecting to localhost:2181
    Welcome to ZooKeeper!
    JLine support is disabled
    WATCHER::
    WatchedEvent state:SyncConnected type:None path:null
    ```
   - Get IDs of Currently Active Brokers ```ls /brokers/ids```
   - Output Should look like: ```[1, 2, 3]```
   - You can close the zookeeper cli if all are working fine
4. Publish Messages With Console Producer
   1. Login to one of the kafka broker ```docker exec -it kafka2 /bin/bash```
   2. Create a new topic called randomTopic:
   <br>```kafka-topics --bootstrap-server localhost:9092 --create --topic randomTopic```
   3. Start Console Producer:
   <br>```kafka-console-producer --bootstrap-server localhost:9092 --topic randomTopic```
5. Read Messages With Console Consumer
   1. Login to one of the kafka broker ```docker exec -it kafka3 /bin/bash```
   2. Start Console Consumer:
      <br>```kafka-console-consumer --bootstrap-server localhost:9092 --topic randomTopic --from-beginning```
   3. You should be able to see the messages which you have published on the same topic
   

**Other kafka commands**
- Create topic with replication factor ```kafka-topics --bootstrap-server localhost:9092 --create --topic testTopic --partitions 2 --replication-factor 3```
- Describe topic ```kafka-topics --bootstrap-server localhost:9092 --describe --topic testTopic```