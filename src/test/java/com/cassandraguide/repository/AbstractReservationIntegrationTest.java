package com.cassandraguide.repository;

import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.GenericContainer;

import com.cassandraguide.conf.CassandraConfiguration;
import com.cassandraguide.model.Reservation;

/**
 * Integration test for implementations of {@link ReservationRepository}. We expect to run 
 * the same tests for 3 differents implementation (Simple, QueryBuilder and Mapper). This class
 * will have all the logic and child classes will only initialized proper implementation.s 
 * 
 * REQUIRED ; DOCKER MUST BE STARTED
 * 
 * <p>We start a Cassandra container using TestContainers (mapping a random host and port).
 * <p>There will be a single Container and schema is reinitialized at each method (to speed up).
 * <p>This unit test does not require a Spring application context to be created.
 *
 * @author Jeffrey CARPENTER (@jscarp)
 * @author Cedrick LUNVEN (@clunven)
 */
public abstract class AbstractReservationIntegrationTest {
    
    /**
     * Singleton Pattern avoid waiting container init for each method
     */
    protected static CassandraConfiguration cassandraConfig    = null;
    protected static GenericContainer<?>    cassandraContainer = null;
    
    /** 
     * Initialize repository once as well against cassandra docker container
     * random port and hostname 
     */
    @BeforeAll
    public static void _initReservationRepository() {
        cassandraContainer = new CassandraContainer<>("cassandra:3.11.4");
        cassandraContainer.start();
        cassandraConfig = new CassandraConfiguration();
        cassandraConfig.setDropSchema(true);
        cassandraConfig.setCassandraHost(cassandraContainer.getContainerIpAddress());
        cassandraConfig.setCassandraPort(cassandraContainer.getMappedPort(9042));
    }
    
    /** To be implemented by sub classes. */
    protected abstract ReservationRepository initReservationRepository();
    
    protected ReservationRepository reservationRepo = null;
    
    /*
     * ReCreate keyspace and table before each test
     */
    @BeforeEach
    public void _recreateSchema() {
        reservationRepo = initReservationRepository();
        reservationRepo.createTables(cassandraConfig.cqlSession(), cassandraConfig.keyspace());
    }
    
    @Test
    @DisplayName("Creating a new reservation")
    public void upsertNewReservation_should_insert_entry() {
        // Given a confirmation number
        String confirmationNumber = UUID.randomUUID().toString();
        // That does not exist in DB
        Assertions.assertFalse(reservationRepo.exists(confirmationNumber));
        Assertions.assertFalse(reservationRepo.findByConfirmationNumber(confirmationNumber).isPresent());
        // When upserting a reservation with this number
        Reservation r1 = new Reservation();
        r1.setConfirmationNumber(confirmationNumber);
        r1.setEndDate(LocalDate.of(2020, 12, 20));
        r1.setStartDate(LocalDate.now());
        r1.setHotelId("12345");
        r1.setGuestId(UUID.randomUUID());
        r1.setRoomNumber(Short.valueOf("42"));
        reservationRepo.upsert(r1);
        // A new entry is created
        Assertions.assertTrue(reservationRepo.exists(confirmationNumber));
        // And can be retrieved
        Assertions.assertTrue(reservationRepo.findByConfirmationNumber(confirmationNumber).isPresent());
    }
    
    @Test
    @DisplayName("Updating an existing Reservation")
    public void upsertExistingReservation_should_update_entry() {
        // Given an existing reservation
        Reservation r1 = new Reservation();
        r1.setEndDate(LocalDate.of(2020, 12, 20));
        r1.setStartDate(LocalDate.now());
        r1.setHotelId("12345");
        r1.setGuestId(UUID.randomUUID());
        r1.setRoomNumber(Short.valueOf("42"));
        String confirmationNumber = reservationRepo.upsert(r1);
        Assertions.assertTrue(reservationRepo.exists(confirmationNumber));
        
        // When upserting with same confirmation number
        Reservation r2 = new Reservation();
        r2.setConfirmationNumber(confirmationNumber);
        r2.setEndDate(LocalDate.of(2020, 12, 20));
        r2.setStartDate(LocalDate.now());
        r2.setHotelId("9999");  // <-- updating hotel ID
        r2.setGuestId(UUID.randomUUID());
        r2.setRoomNumber(Short.valueOf("42"));
        reservationRepo.upsert(r2);
        
        // Record should have been updated 
        Optional<Reservation> r3 = reservationRepo.findByConfirmationNumber(confirmationNumber);
        Assertions.assertTrue(r3.isPresent());
        Assertions.assertEquals(r2.getHotelId(), r3.get().getHotelId());
    }
    
    @Test
    @DisplayName("Deleting an existing Reservation")
    public void deleteExistingReservation_should_empty_Table() {
        // Given an existing reservation
        Reservation r1 = new Reservation();
        r1.setEndDate(LocalDate.of(2020, 12, 20));
        r1.setStartDate(LocalDate.now());
        r1.setHotelId("12345");
        r1.setGuestId(UUID.randomUUID());
        r1.setRoomNumber(Short.valueOf("42"));
        String confirmationNumber = reservationRepo.upsert(r1);
        Assertions.assertTrue(reservationRepo.exists(confirmationNumber));
        // When deleting by id
        reservationRepo.delete(confirmationNumber);
        // The record is no more present
        Assertions.assertFalse(reservationRepo.exists(confirmationNumber));
    }
    
    @Test
    @DisplayName("Create 2 reservations")
    public void upsert2Reservations_should_fill_table() {
        // Given an existing reservation
        Reservation r1 = new Reservation();
        r1.setEndDate(LocalDate.of(2020, 12, 20));
        r1.setStartDate(LocalDate.now());
        r1.setHotelId("12345");
        r1.setGuestId(UUID.randomUUID());
        r1.setRoomNumber(Short.valueOf("42"));
        // When
        String confirmationNumber1 = reservationRepo.upsert(r1);
        r1.setConfirmationNumber(null);
        String confirmationNumber2 = reservationRepo.upsert(r1);
        // Then
        Assertions.assertTrue(reservationRepo.exists(confirmationNumber1));
        Assertions.assertTrue(reservationRepo.exists(confirmationNumber2));
        // The record is no more present
        Assertions.assertEquals(2, reservationRepo.findAll().size());
    }

}

