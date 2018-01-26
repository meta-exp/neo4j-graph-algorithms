FROM neo4j:3.3-enterprise
RUN apk --update add maven openjdk8
ADD . /neo4j-graph-algorithms/
WORKDIR neo4j-graph-algorithms
RUN mvn clean install
RUN cp algo/target/graph-algorithms-*.jar /var/lib/neo4j/plugins/

# TODO: I think we would have to use the file from the neo4j repo
ENTRYPOINT ["/docker-entrypoint.sh"]
# CMD ["neo4j console"]
#ENTRYPOINT ["neo4j", "console"]