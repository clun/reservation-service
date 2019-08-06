package com.cassandraguide.repository;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import com.cassandraguide.model.Reservation;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * Implementation Reservation methods with Mapper.
 */
@Repository("reservation.repository.simple")
@Profile("!unit-test") // When I do some 'unit-test' no connectivity to DB
public class ReservationRepositorySimpleStatement implements ReservationRepository {

    /** Logger for the class. */
    private static final Logger logger = LoggerFactory.getLogger(ReservationRepositorySimpleStatement.class);
    
    /** CqlSession holding metadata to interact with Cassandra. */
    private CqlSession     cqlSession;
    
    /** External Initialization. */
    public ReservationRepositorySimpleStatement(
            @NonNull CqlSession cqlSession, 
            @NonNull @Qualifier("keyspace") CqlIdentifier keyspaceName) {
        this.cqlSession   = cqlSession;
        createTables(cqlSession, keyspaceName);
        logger.info("Application initialized.");
    }
    
    /**
     * Dedicated constructor for test not creating tables.
     */
    public ReservationRepositorySimpleStatement(
            @NonNull CqlSession cqlSession, 
            @NonNull @Qualifier("keyspace") CqlIdentifier keyspaceName, boolean test) {
        this.cqlSession   = cqlSession;
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean exists(String confirmationNumber) {
        Assert.hasLength(confirmationNumber, "ConfirmationNumber should not be null nor empty");
        return cqlSession.execute(SimpleStatement.builder(
                "SELECT confirmation_number FROM reservations_by_confirmation WHERE confirmation_number = ?")
                .addPositionalValue(confirmationNumber)
                .build())
                .getAvailableWithoutFetching() > 0;
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Reservation> findByConfirmationNumber(String confirmationNumber) {
        Assert.hasLength(confirmationNumber, "ConfirmationNumber should not be null nor empty");
        SimpleStatement ssFindByConfirmationNumber = SimpleStatement.builder(
                "SELECT * FROM reservations_by_confirmation WHERE confirmation_number = :num")
                .addNamedValue("num", confirmationNumber) // Hint: Instead of using position you can use names
                .build();
        
        ResultSet resultSet = cqlSession.execute(ssFindByConfirmationNumber);
        
        // Hint: an empty result might not be an error as this method is sometimes used to check whether a
        // reservation with this confirmation number exists
        Row row = resultSet.one();
        if (row == null) {
            logger.debug("Unable to load reservation with confirmation number: " + confirmationNumber);
            return Optional.empty();
        }
        
        // Hint: If there is a result, create a new reservation object and set the values
        // Bonus: factor the logic to extract a reservation from a row into a separate method
        // (you will reuse it again later in getAllReservations())
        return Optional.of(mapRowToReservation(row));
    }

    /** {@inheritDoc} */
    @Override
    public String upsert(Reservation reservation) {
        Assert.notNull(reservation, "reservation should not be null");
        
        if (null == reservation.getConfirmationNumber()) {
            // Generating a new reservation number if none has been provided
            reservation.setConfirmationNumber(UUID.randomUUID().toString());
        }
        
        // SimpleStatement to insert into 'reservations_by_hotel_date'
        SimpleStatement ssInsertReservationByHotelDate = SimpleStatement.builder(
                "INSERT INTO reservations_by_hotel_date (confirmation_number, hotel_id, start_date, " +
                        "end_date, room_number, guest_id) VALUES (?, ?, ?, ?, ?, ?)")
                .addPositionalValue(reservation.getConfirmationNumber())
                .addPositionalValue(reservation.getHotelId())
                .addPositionalValue(reservation.getStartDate())
                .addPositionalValue(reservation.getEndDate())
                .addPositionalValue(reservation.getRoomNumber())
                .addPositionalValue(reservation.getGuestId())
                .build();

        // SimpleStatement to insert into 'reservations_by_hotel_date'
        SimpleStatement ssInsertRreservationByConfirmation = SimpleStatement.builder(
                 "INSERT INTO reservations_by_confirmation (confirmation_number, hotel_id, start_date, " +
                         "end_date, room_number, guest_id) VALUES (?, ?, ?, ?, ?, ?)")
                 .addPositionalValue(reservation.getConfirmationNumber())
                 .addPositionalValue(reservation.getHotelId())
                 .addPositionalValue(reservation.getStartDate())
                 .addPositionalValue(reservation.getEndDate())
                 .addPositionalValue(reservation.getRoomNumber())
                 .addPositionalValue(reservation.getGuestId())
                 .build();

        // Group Statements in a Batch
        cqlSession.execute(BatchStatement
                .builder(DefaultBatchType.LOGGED)
                .addStatement(ssInsertReservationByHotelDate)
                .addStatement(ssInsertRreservationByConfirmation)
                .build());
        
        return reservation.getConfirmationNumber();
    }

    /** {@inheritDoc} */
    @Override
    public List<Reservation> findAll() {
        // SimpleStatement to read all rows from 'reservations_by_confirmation'
        SimpleStatement ssFindAll = SimpleStatement.newInstance("SELECT * FROM reservations_by_confirmation");

        return cqlSession.execute(ssFindAll)
                  .all()                          // no paging we retrieve all objects
                  .stream()                       // because we are good people
                  .map(this::mapRowToReservation) // Mapping row as Reservation
                  .collect(Collectors.toList());  // Back to list objects
    }

    /** {@inheritDoc} */
    @Override
    public void delete(String confirmationNumber) {
        Assert.hasLength(confirmationNumber, "ConfirmationNumber cannot be null nor empty");
        Optional<Reservation> existingReservation = findByConfirmationNumber(confirmationNumber);
        if (!existingReservation.isEmpty()) {
            
            Reservation reservation = existingReservation.get();
            
            // Create SimpleStatement to delete from 'reservations_by_hotel_date'
            SimpleStatement ssDeleteReservationByHotelDate = SimpleStatement.builder(
                    "DELETE FROM reservations_by_hotel_date WHERE hotel_id = ? AND start_date = ? AND room_number = ?")
                    .addPositionalValue(reservation.getHotelId())
                    .addPositionalValue(reservation.getStartDate())
                    .addPositionalValue(reservation.getRoomNumber())
                    .build();
            
            // Create SimpleStatement to delete from 'reservations_by_confirmation'
            SimpleStatement ssDeleteReservationByConfirmation = SimpleStatement.builder(
                    "DELETE FROM reservations_by_confirmation WHERE confirmation_number = ?")
                    .addPositionalValue(reservation.getConfirmationNumber())
                    .build();
            
            cqlSession.execute(BatchStatement
                .builder(DefaultBatchType.LOGGED)
                .addStatement(ssDeleteReservationByHotelDate)
                .addStatement(ssDeleteReservationByConfirmation)
                .build());
        };
    }

    /** {@inheritDoc} */
    @Override
    public List<Reservation> findByHotelAndDate(String hotelId, LocalDate localDate) {
        Assert.hasLength(hotelId, "Hotel Id should not be null nor empty");
        Assert.notNull(localDate, "Local Date object should not be null nor empty");
   
        // Create SimpleStatement to search 'reservations_by_hotel_date'
        SimpleStatement ssSearchReservationByHotelDate = SimpleStatement.builder(
                "SELECT * FROM reservations_by_hotel_date WHERE hotel_id = ? AND start_date = ?")
                .addPositionalValue(hotelId)
                .addPositionalValue(localDate)
                .build();

        return cqlSession.execute(ssSearchReservationByHotelDate)
                         .all()                          // no paging we retrieve all objects
                         .stream()                       // because we are good people
                         .map(this::mapRowToReservation) // Mapping row as Reservation
                         .collect(Collectors.toList());  // Back to list objects
    }

}
