package com.cassandraguide.repository;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

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

/**
 * Implementation Reservation methods with Mapper.
 */
@Repository("reservation.repository.simple")
@Profile("!unit-test") // When I do some 'unit-test' no connectivity to DB
public class ReservationRepositorySimple implements ReservationRepository {

    /** Logger for the class. */
    private static final Logger logger = LoggerFactory.getLogger(ReservationRepositorySimple.class);
    
    /** External Initialization. */
    public ReservationRepositorySimple(
            @NonNull CqlSession cqlSession, 
            @NonNull @Qualifier("keyspace") CqlIdentifier keyspaceName) {
        createTables(cqlSession, keyspaceName);
        logger.info("Application initialized.");
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean exists(String confirmationNumber) {
        Assert.hasLength(confirmationNumber, "ConfirmationNumber should not be null nor empty");
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Reservation> findByConfirmationNumber(String confirmationNumber) {
        Assert.hasLength(confirmationNumber, "ConfirmationNumber should not be null nor empty");
        
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public String upsert(Reservation res) {
        Assert.notNull(res, "reservation should not be null");
        if (null == res.getConfirmationNumber()) {
            // Generating a new reservation number if none has been provided
            res.setConfirmationNumber(UUID.randomUUID().toString());
        }
        return res.getConfirmationNumber();
    }

    /** {@inheritDoc} */
    @Override
    public List<Reservation> findAll() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void delete(String confirmationNumber) {
        Assert.hasLength(confirmationNumber, "ConfirmationNumber cannot be null nor empty");
    }

    /** {@inheritDoc} */
    @Override
    public List<Reservation> findByHotelAndDate(String hotelId, LocalDate localDate) {
        Assert.hasLength(hotelId, "Hotel Id should not be null nor empty");
        Assert.notNull(localDate, "Local Date object should not be null nor empty");
        return null;
    }

}
