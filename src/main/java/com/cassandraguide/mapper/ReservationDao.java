package com.cassandraguide.mapper;

import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

import com.cassandraguide.model.Reservation;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.StatementAttributes;

/**
 * Using Java driver v2 mapping and annotation processing.
 */
@Dao
public interface ReservationDao {
    
    /** Search in table reservations_by_confirmation and retrieve record (if exist) by the PK. */
    @Select
    @StatementAttributes(consistencyLevel = "ONE")
    Optional<ReservationByConfirmationEntity> findByConfirmation(String confirmationNumber);
    
    /** Accessing resulset is enough to evaluate existence leveraging on 'rs.one()' operation.*/
    @Query("SELECT confirmation_number "
         + "FROM reservation.reservations_by_confirmation "
         + "WHERE confirmation_number = :myConfNumber")
    ResultSet existConfirmation(String myConfNumber);
    
    /** Insert into reservations_by_confirmation providing all values with the entity. */
    @Insert
    void upsertReservationByConfirmation(ReservationByConfirmationEntity res);
    
    /** Insert into reservations_by_hotel_day providing all values with the entity. */
    @Insert
    void upsertReservationByHotelDate(ReservationByHotelDateEntity res);
   
    /** 
     * Insert into reservations_by_confirmation AND reservations_by_hotel_day as a single operation using a BATCH. 
     * 
     * Notice that, even if same parameters are used in multiple queries we need still to pass them as separated argument for each
     * to ensure uniqueness of the placeholder (ex :confNum and :confNum2). It works but for this denormalizing
     * tables it is recommended to use {@link QueryProvider}. Please see next method.
     **/
    @Query("BEGIN BATCH \n"
            + " INSERT INTO reservation.reservations_by_confirmation(confirmation_number, hotel_id, room_number, start_date, end_date, guest_id) "
            + " VALUES(:confNum, :hotel_id, :room, :start, :end, :guestId);\n"
            + " INSERT INTO reservation.reservations_by_hotel_date(hotel_id, start_date, room_number, confirmation_number, end_date, guest_id) "
            + " VALUES(:hotel_id2, :start2, :room2, :confNum2, :end2, :guestId2);\n"
            + "APPLY BATCH")
    ResultSet upsertWithQueryBatch(String confNum, 
            String hotel_id, Short room, 
            LocalDate start,  LocalDate end, UUID guestId,
            String confNum2, String hotel_id2, Short room2, 
            LocalDate start2,  LocalDate end2, UUID guestId2);
    
    /**
     * Implementation has been delegated to a Provider.
     * @see ReservationDaoHelper
     */
    @QueryProvider(
      providerClass = ReservationDaoHelper.class,
      entityHelpers = { ReservationByHotelDateEntity.class, ReservationByConfirmationEntity.class})
    void upsertWithQueryProvider(Reservation res);
    
    /**
     * Find All. For now select is limitating to single element.
     */
    @Query("SELECT * FROM ${keyspaceId}.${tableId}")
    PagingIterable<ReservationByConfirmationEntity> findAll();
    
    @Delete
    void deleteReservationByConfirmation(ReservationByConfirmationEntity res);
    
    @Delete
    void deleteReservationByHotelDate(ReservationByHotelDateEntity res);
    
    @QueryProvider(
            providerClass = ReservationDaoHelper.class,
            entityHelpers = { ReservationByHotelDateEntity.class, ReservationByConfirmationEntity.class})
    void deleteReservation(Reservation res);
    
    @Query("SELECT * "
         + "FROM ${keyspaceId}.${tableId} "
         + "WHERE hotel_id = :hotel_id "
         + "AND start_date = :start_date")
    PagingIterable<ReservationByHotelDateEntity> findByHotelAndDate(String hotel_id, LocalDate start_date);
    
}
