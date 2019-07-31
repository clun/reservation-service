package com.cassandraguide.mapper;

import com.cassandraguide.model.Reservation;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

/**
 * Entity for Cassandra ORM.
 *
 * ---------------------------------------------------------
 * Related Table :
 * ---------------------------------------------------------
 * CREATE TABLE reservation.reservations_by_confirmation (
 *   confirmation_number text PRIMARY KEY,
 *   hotel_id text,
 *   start_date date,
 *   end_date date,
 *   room_number smallint,
 *   guest_id uuid
 * );
 */
@Entity
@CqlName("reservations_by_confirmation")
public class ReservationByConfirmationEntity extends AbstractReservationEntity {

    /** Serial. */
    private static final long serialVersionUID = 6918460063213507346L;
    
    public ReservationByConfirmationEntity() {}
    public ReservationByConfirmationEntity(Reservation r) { super(r); }
    
    @Override
    @PartitionKey
    public String getConfirmationNumber() {
        return confirmationNumber;
    }

}
