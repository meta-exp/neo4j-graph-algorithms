FROM neo4j:3.3-enterprise as neo4j
RUN apk --update add maven openjdk8 
RUN echo "dbms.connectors.default_listen_address=0.0.0.0" >> /var/lib/neo4j/conf/neo4j.conf
RUN echo "dbms.connector.http.listen_address=0.0.0.0:7474" >> /var/lib/neo4j/conf/neo4j.conf
RUN echo "dbms.connector.https.listen_address=0.0.0.0:7473" >> /var/lib/neo4j/conf/neo4j.conf
RUN echo "dbms.connector.bolt.listen_address=0.0.0.0:7687" >> /var/lib/neo4j/conf/neo4j.conf
ADD . /neo4j-graph-algorithms/
WORKDIR /neo4j-graph-algorithms/
RUN mvn -Dmaven.test.skip=true clean install
RUN cp algo/target/graph-algorithms-*.jar /var/lib/neo4j/plugins/

EXPOSE 7474 7473 7687

WORKDIR /var/lib/neo4j

VOLUME /data

COPY --from=neo4j docker-entrypoint.sh /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["neo4j"]
