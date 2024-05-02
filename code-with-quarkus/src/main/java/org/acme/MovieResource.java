package org.acme;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.context.ThreadContext;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;


@Path("/movie")
public class MovieResource {

    @ConfigProperty(name = "database.name") 
    String databaseName;


    @Inject
    Driver driver;

    @Inject
    ThreadContext threadContext;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<Map<String,Object>> getMovies(@DefaultValue("10") @QueryParam("limit") Integer limit ) {
        try (Session session = driver.session(SessionConfig.forDatabase(databaseName))) {
           return session.executeRead(tx -> {
               var result = tx.run("""
                    match (m:Movie) 
                    return {title:m.title, actors: [ (m)<-[:ACTED_IN]-(a) | a.name] } as result
                    order by m.title asc 
                    limit $limit""", 
               Map.of("limit", limit));
               return result.stream().map(record -> {
                   return record.get("result").asMap();
               }).collect(Collectors.toList());
           });
        }
    }
}
