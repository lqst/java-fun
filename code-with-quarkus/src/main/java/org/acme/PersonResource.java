package org.acme;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.context.ThreadContext;
import org.jboss.resteasy.reactive.Separator;
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

@Path("/person")
public class PersonResource {
     @ConfigProperty(name = "skills.database.name") 
    String databaseName;


    @Inject
    Driver driver;


    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<Map<String,Object>> getPersons(
        @DefaultValue("3") @QueryParam("limit") Integer limit,
        @DefaultValue("false") @QueryParam("fuzzy") Boolean fuzzy,
        @QueryParam("skills") @Separator(",") Set<String> skills) {
        try (Session session = driver.session(SessionConfig.forDatabase(databaseName))) {
            if (skills == null) return Collections.emptyList();
            if (fuzzy) {
                return session.executeRead(tx -> {
                    var result = tx.run("""
                        unwind $requested_skills as wanted_skill
                        match (s:Skill) where s.norm_name contains toLower(wanted_skill)
                        with collect(s) as wanted_skills
                        with wanted_skills
                        unwind wanted_skills as s
                        match (s)-[:KNOWS|SIMILAR_EMBEDDING*1..2]-(p:Person)
                        with p, collect(distinct s) as matched_skills
                        return {
                            name: p.name , 
                            skills: [(p)-[:KNOWS]->(skill) | skill.name], 
                            no_matched_skills: size(matched_skills),
                            matched_skills: [x in matched_skills | x.name]
                        } as result 
                        order by size(matched_skills) desc limit $limit
                         """, 
                    Map.of("limit", limit,
                     "requested_skills", skills));
                    return result.stream().map(record -> {
                        return record.get("result").asMap();
                    }).collect(Collectors.toList());
                });
            } else {
                return session.executeRead(tx -> {
                    var result = tx.run("""
                         unwind $requested_skills as wanted_skill
                         match (s:Skill) where s.norm_name contains toLower(wanted_skill)
                         with s
                         match (s)<-[:KNOWS]-(p:Person)
                         with p, collect(distinct s) as matched_skills
                         return {
                             name: p.name , 
                             skills: [(p)-[:KNOWS]->(skill) | skill.name], 
                             no_matched_skills: size(matched_skills),
                             matched_skills: [x in matched_skills | x.name]
                         } as result 
                         order by size(matched_skills) desc limit $limit
                         """, 
                    Map.of("limit", limit,
                     "requested_skills", skills));
                    return result.stream().map(record -> {
                        return record.get("result").asMap();
                    }).collect(Collectors.toList());
                });
            }

           
        }
    }
}
