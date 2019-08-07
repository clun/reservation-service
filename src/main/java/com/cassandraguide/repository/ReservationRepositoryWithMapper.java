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

import com.cassandraguide.mapper.ReservationByConfirmationEntity;
import com.cassandraguide.mapper.ReservationByHotelDateEntity;
import com.cassandraguide.mapper.ReservationDao;
import com.cassandraguide.mapper.ReservationMapperBuilder;
import com.cassandraguide.model.Reservation;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Implementation Reservation methods with Mapper.
 */
@Repository("reservation.repository.mapper")
@Profile("!unit-test") // When I do some 'unit-test' no connectivity to DB
public class ReservationRepositoryWithMapper implements ReservationRepository {

    /** Logger for the class. */
    private static final Logger logger = LoggerFactory.getLogger(ReservationRepositoryWithMapper.class);
    
    /** CqlSession holding metadata to interact with Cassandra. */
    private ReservationDao reservationDao;
    
    /** External Initialization. */
    public ReservationRepositoryWithMapper(
            @NonNull CqlSession cqlSession, 
            @NonNull @Qualifier("keyspace") CqlIdentifier keyspaceName) {
        createTables(cqlSession, keyspaceName);
        reservationDao = 
                new ReservationMapperBuilder(cqlSession).build().reservationDao(keyspaceName);
        logger.info("Application initialized.");
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean exists(String confirmationNumber) {
        Assert.hasLength(confirmationNumber, "ConfirmationNumber should not be null nor empty");
        return (reservationDao.existConfirmation(confirmationNumber).one() != null);
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Reservation> findByConfirmationNumber(String confirmationNumber) {
        Assert.hasLength(confirmationNumber, "ConfirmationNumber should not be null nor empty");
        Optional<ReservationByConfirmationEntity> daoRes = reservationDao.findByConfirmation(confirmationNumber);
        return (daoRes.isPresent()) ? Optional.ofNullable(daoRes.get().asReservation()) : Optional.empty();
    }
    
    /** {@inheritDoc} */
    @Override
    public String upsert(Reservation res) {
        
        Assert.notNull(res, "reservation should not be null");
        if (null == res.getConfirmationNumber()) {
            // Generating a new reservation number if none has been provided
            res.setConfirmationNumber(UUID.randomUUID().toString());
        }
        
        // --> (1) This is working but this is NOT what you expect => you want a batch statement
        //reservationDao.upsertReservationByConfirmation(new ReservationByConfirmationEntity(res));
        //reservationDao.upsertReservationByHotelDate(new ReservationByHotelDateEntity(res));
        // <--
        
        // --> (2) This is working but this is NOT optimal due to arguments duplication
        //reservationDao.upsertWithQueryBatch(
        //        res.getConfirmationNumber(), res.getHotelId(), res.getRoomNumber(), 
        //        res.getStartDate(), res.getEndDate(), res.getGuestId(),
        //        res.getConfirmationNumber(), res.getHotelId(), res.getRoomNumber(), 
        //        res.getStartDate(), res.getEndDate(), res.getGuestId());
        // <--
        
        // --> (3) This is the best choice for denormalizing tables
        reservationDao.upsertWithQueryProvider(res);
        // <--
        
        return res.getConfirmationNumber();
    }

    /** {@inheritDoc} */
    @Override
    public List<Reservation> findAll() {
        return reservationDao.findAll()             // Get first page from Query
                .all().stream()                     // There are not billions of records, give me them all
                .map(ReservationByConfirmationEntity::asReservation) // Map as Reservation
                .collect(Collectors.toList());      // Collect as expected list
    }

    /** {@inheritDoc} */
    @Override
    public void delete(String confirmationNumber) {
        Assert.hasLength(confirmationNumber, "ConfirmationNumber should not be null nor empty");
        findByConfirmationNumber(confirmationNumber).ifPresent(reservationDao::deleteReservation);
    }

    /** {@inheritDoc} */
    @Override
    public List<Reservation> findByHotelAndDate(String hotelId, LocalDate localDate) {
        Assert.hasLength(hotelId, "Hotel Id should not be null nor empty");
        Assert.notNull(localDate, "Local Date object should not be null nor empty");
        return reservationDao.findByHotelAndDate(hotelId, localDate)
                .all().stream()                                   // Because we are good people
                .map(ReservationByHotelDateEntity::asReservation) // Mapping row as Reservation
                .collect(Collectors.toList());                    // Back to list objects
    }

}
