package com.cassandraguide.repository;

import java.time.LocalDate;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.cassandraguide.conf.CassandraConfiguration;
import com.cassandraguide.model.Reservation;
import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Please start a docker container before existing those tests.
 *
 * @author Cedrick LUNVEN (@clunven)
 */
public class SimpleTests {
    
    protected static CassandraConfiguration cc;
    protected static CqlSession             cqlSession;
    
    protected static ReservationRepository  repoSimple;
    protected static ReservationRepository  repoMapper;
    protected static ReservationRepository  repoQueryBuilder;
    
    String myConfirmationNumber = "117ba932-7809-4ca0-8d84-88c2b77982a8";
    
    @BeforeAll
    public static void _initConnection() {
        cc               = new CassandraConfiguration("localhost", 9042, "datacenter1", "reservation", false);
        cqlSession       = cc.cqlSession();
        repoMapper       = new ReservationRepositoryWithMapper(cqlSession, cc.keyspace());
        repoQueryBuilder = new ReservationRepositoryWithQueryBuilder(cqlSession, cc.keyspace());
        repoSimple       = new ReservationRepositorySimple(cqlSession, cc.keyspace());
    }
    
    @Test
    @DisplayName("Samples Tests")
    public void testConnectivity() {
        Reservation r1 = new Reservation();
        r1.setEndDate(LocalDate.of(2019, 12, 20));
        r1.setStartDate(LocalDate.now());
        r1.setHotelId("12345");
        r1.setGuestId(UUID.randomUUID());
        r1.setRoomNumber(Short.valueOf("42"));
        r1.setConfirmationNumber(myConfirmationNumber);
        repoMapper.upsert(r1);
        //r1.setConfirmationNumber(UUID.randomUUID().toString());
        //repoMapper.upsert(r1);
        //System.out.println(repoMapper.exists(myConfirmationNumber));
        //System.out.println(repoMapper.findByConfirmationNumber(myConfirmationNumber));
        //System.out.println(repoMapper.findAll());
    }

}
