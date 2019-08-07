/*
 * Copyright (C) 2017 Jeff Carpenter
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cassandraguide.repository;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.relation.Relation.column;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.PreDestroy;

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
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

/**
 * The goal of this project is to provide a minimally functional implementation of a microservice 
 * that uses Apache Cassandra for its data storage. The reservation service is implemented as a 
 * RESTful service using Spring Boot.
 */
@Repository("reservation.repository.querybuilder")
@Profile("!unit-test") // When I do some 'unit-test' no connectivity to DB
public class ReservationRepositoryWithQueryBuilder implements ReservationRepository {

    /** Logger for the class. */
    private static final Logger logger = LoggerFactory.getLogger(ReservationRepositoryWithQueryBuilder.class);
    
    /** CqlSession holding metadata to interact with Cassandra. */
    private CqlSession     cqlSession;
    private CqlIdentifier  keyspaceName;
    
    private PreparedStatement psExistReservation;
    private PreparedStatement psFindReservation;
    private PreparedStatement psSearchReservation;
    private PreparedStatement psInsertReservationByHotelDate;
    private PreparedStatement psInsertReservationByConfirmation;
    private PreparedStatement psDeleteReservationByHotelDate;
    private PreparedStatement psDeleteReservationByConfirmation;
    
    /**
     * Default constructor is required for java reflection and Injection.
     */
    public ReservationRepositoryWithQueryBuilder() {
    }
    
    /** External Initialization. */
    public ReservationRepositoryWithQueryBuilder(
            @NonNull CqlSession cqlSession, 
            @NonNull @Qualifier("keyspace") CqlIdentifier keyspaceName) {
        this.cqlSession   = cqlSession;
        this.keyspaceName = keyspaceName;
        
        // Will create tables (if they do not exist)
        createTables(cqlSession, keyspaceName);
        
        // Prepare Statements of reservation
        prepareStatements();
        logger.info("Application initialized.");
    }
    
    /**
     * Initializing with also prepared statements (MOCKING).
     * @param cqlSession
     *          cql session
     * @param keyspaceName
     *          target keyspace
     * @param psExistReservation
     *          test if a reservation exists
     * @param psFindReservation
     *          find a reservation by its id
     * @param psInsertReservationByHotel
     *          insert a new reservation
     * @param psInsertReservationByConfirmation
     *          
     * @param psDeleteReservation
     * @param psSearchReservation
     */
    public ReservationRepositoryWithQueryBuilder(@NonNull CqlSession cqlSession, 
                                 @Qualifier("keyspace") @NonNull CqlIdentifier keyspaceName, 
                                 @NonNull PreparedStatement psExistReservation,
                                 @NonNull PreparedStatement psFindReservation,
                                 @NonNull PreparedStatement psInsertReservationByHotel,
                                 @NonNull PreparedStatement psInsertReservationByConfirmation,
                                 @NonNull PreparedStatement psDeleteReservationByHotel,
                                 @NonNull PreparedStatement psDeleteReservationByConfirmation,
                                 @NonNull PreparedStatement psSearchReservation) {
        this.cqlSession                         = cqlSession;
        this.keyspaceName                       = keyspaceName;
        this.psExistReservation                 = psExistReservation;
        this.psFindReservation                  = psFindReservation;
        this.psInsertReservationByHotelDate     = psInsertReservationByHotel;
        this.psInsertReservationByConfirmation  = psInsertReservationByConfirmation;
        this.psDeleteReservationByHotelDate     = psDeleteReservationByHotel;
        this.psDeleteReservationByConfirmation  = psDeleteReservationByConfirmation;
        this.psSearchReservation                = psSearchReservation;
        logger.info("Application initialized (MOCK)");
    }
    
    /**
     * CqlSession is a stateful object handling TCP connection.
     * You may want to properly close sockets when you close you application
     */
    @PreDestroy
    public void cleanup() {
        if (null != cqlSession) {
            cqlSession.close();
            logger.info("+ CqlSession has been successfully closed");
        }
    }
    
    /** {@inheritDoc} */
    public boolean exists(String confirmationNumber) {
        Assert.hasLength(confirmationNumber, "ConfirmationNumber should not be null nor empty");
        return cqlSession.execute(psExistReservation.bind(confirmationNumber))
                         .getAvailableWithoutFetching() > 0;
    }
   
    /** {@inheritDoc} */
    public Optional<Reservation> findByConfirmationNumber(String confirmationNumber) {
        Assert.hasLength(confirmationNumber, "ConfirmationNumber should not be null nor empty");
        ResultSet resultSet = cqlSession.execute(psFindReservation.bind(confirmationNumber));
        // Hint: an empty result might not be an error as this method is sometimes used to check whether a
        // reservation with this confirmation number exists
        Row row = resultSet.one();
        if (row == null) {
            logger.debug("Unable to load reservation with confirmation number: " + confirmationNumber);
            return Optional.empty();
        }
        // Hint: If there is a result, create a new reservation object and set the values
        // Hint: use provided convenience function convertDataStaxLocalDateToJava for start and end dates
        // Bonus: factor the logic to extract a reservation from a row into a separate method
        // (you will reuse it again later in getAllReservations())
        return Optional.of(mapRowToReservation(row));
    }
    
    /**
     * Create new entry in multiple tables for this reservation.
     *
     * @param reservation
     *      current reservation object
     * @return
     *      reservation confirmation number
     */
     public String upsert(Reservation r) {
        Assert.notNull(r, "Reservation object should not be null nor empty");
        if (null == r.getConfirmationNumber()) {
            // Generating a new reservation number if none has been provided
            r.setConfirmationNumber(UUID.randomUUID().toString());
        }
        cqlSession.execute(BatchStatement
                .builder(DefaultBatchType.LOGGED)
                .addStatement(psInsertReservationByHotelDate.bind(
                                  r.getHotelId(), r.getStartDate(), r.getEndDate(), 
                                  r.getRoomNumber(), r.getConfirmationNumber(), r.getGuestId()))
                .addStatement(psInsertReservationByConfirmation.bind(
                                  r.getConfirmationNumber(), r.getHotelId(), r.getStartDate(), 
                                  r.getEndDate(), r.getRoomNumber(), r.getGuestId()))
                .build());
        return r.getConfirmationNumber();
    }
    
    /** {@inheritDoc} */
    public List<Reservation> findAll() {
        return cqlSession.execute(selectFrom(keyspaceName, TABLE_RESERVATION_BY_CONFI).all().build())
                  .all()                          // no paging we retrieve all objects
                  .stream()                       // because we are good people
                  .map(this::mapRowToReservation) // Mapping row as Reservation
                  .collect(Collectors.toList());  // Back to list objects
    }
    
    /** {@inheritDoc} */
    public void delete(String confirmationNumber) {
        Assert.hasLength(confirmationNumber, "ConfirmationNumber should not be null nor empty");
        Optional<Reservation> existingReservation = findByConfirmationNumber(confirmationNumber);
        if (!existingReservation.isEmpty()) {
            Reservation res = existingReservation.get();
            cqlSession.execute(BatchStatement
                .builder(DefaultBatchType.LOGGED)
                .addStatement(psDeleteReservationByConfirmation.bind(res.getConfirmationNumber()))
                .addStatement(psDeleteReservationByHotelDate.bind(res.getHotelId(), res.getStartDate(), res.getRoomNumber()))
                .build());
        };
    }
    
    /** {@inheritDoc} */
    public List<Reservation> findByHotelAndDate(String hotelId, LocalDate localDate) {
        Assert.hasLength(hotelId, "Hotel Id should not be null nor empty");
        Assert.notNull(localDate, "Local Date object should not be null nor empty");
        return cqlSession.execute(psSearchReservation.bind(hotelId, localDate))
                         .all()                          // no paging we retrieve all objects
                         .stream()                       // because we are good people
                         .map(this::mapRowToReservation) // Mapping row as Reservation
                         .collect(Collectors.toList());  // Back to list objects
    }

    
    
    

    private void prepareStatements() {
        if (psExistReservation == null) {
            psExistReservation = cqlSession.prepare(
                                selectFrom(keyspaceName, TABLE_RESERVATION_BY_CONFI).column(CONFIRMATION_NUMBER)
                                .where(column(CONFIRMATION_NUMBER).isEqualTo(bindMarker(CONFIRMATION_NUMBER)))
                                .build());
            psFindReservation = cqlSession.prepare(
                                selectFrom(keyspaceName, TABLE_RESERVATION_BY_CONFI).all()
                                .where(column(CONFIRMATION_NUMBER).isEqualTo(bindMarker(CONFIRMATION_NUMBER)))
                                .build());
            psSearchReservation = cqlSession.prepare(
                                selectFrom(keyspaceName, TABLE_RESERVATION_BY_HOTEL).all()
                                .where(column(HOTEL_ID).isEqualTo(bindMarker(HOTEL_ID)))
                                .where(column(START_DATE).isEqualTo(bindMarker(START_DATE)))
                                .build());
            psDeleteReservationByConfirmation = cqlSession.prepare(
                                deleteFrom(keyspaceName, TABLE_RESERVATION_BY_CONFI)
                                .where(column(CONFIRMATION_NUMBER).isEqualTo(bindMarker(CONFIRMATION_NUMBER)))
                                .build());
            psDeleteReservationByHotelDate = cqlSession.prepare(
                    deleteFrom(keyspaceName, TABLE_RESERVATION_BY_HOTEL)
                    .where(column(HOTEL_ID).isEqualTo(bindMarker(HOTEL_ID)))
                    .where(column(START_DATE).isEqualTo(bindMarker(START_DATE)))
                    .where(column(ROOM_NUMBER).isEqualTo(bindMarker(ROOM_NUMBER)))
                    .build());
            psInsertReservationByHotelDate = cqlSession.prepare(QueryBuilder.insertInto(keyspaceName, TABLE_RESERVATION_BY_HOTEL)
                    .value(HOTEL_ID, bindMarker(HOTEL_ID))
                    .value(START_DATE, bindMarker(START_DATE))
                    .value(END_DATE, bindMarker(END_DATE))
                    .value(ROOM_NUMBER, bindMarker(ROOM_NUMBER))
                    .value(CONFIRMATION_NUMBER, bindMarker(CONFIRMATION_NUMBER))
                    .value(GUEST_ID, bindMarker(GUEST_ID))
                    .build());
            psInsertReservationByConfirmation = cqlSession.prepare(QueryBuilder.insertInto(keyspaceName, TABLE_RESERVATION_BY_CONFI)
                    .value(CONFIRMATION_NUMBER, bindMarker(CONFIRMATION_NUMBER))
                    .value(HOTEL_ID, bindMarker(HOTEL_ID))
                    .value(START_DATE, bindMarker(START_DATE))
                    .value(END_DATE, bindMarker(END_DATE))
                    .value(ROOM_NUMBER, bindMarker(ROOM_NUMBER))
                    .value(GUEST_ID, bindMarker(GUEST_ID))
                    .build());
            logger.info("Statements have been successfully prepared.");
        }
    }
}