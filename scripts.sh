# Use a docker image to have access to all the binaries right away:
docker run -it --rm --net=host confluentinc/cp-schema-registry:3.3.1 bash
# Then you can do
kafka-avro-console-consumer
