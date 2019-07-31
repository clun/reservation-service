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
package com.cassandraguide.mapper;

import java.time.LocalDate;

import com.cassandraguide.model.Reservation;
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

/**
 * Entity for Cassandra ORM.
 *
 * ---------------------------------------------------------
 * Related Table :
 * ---------------------------------------------------------
 * CREATE TABLE reservation.reservations_by_hotel_date (
 *  hotel_id text,
 *  start_date date,
 *  end_date date,
 *  room_number smallint,
 *  confirmation_number text,
 *  guest_id uuid,
 *  PRIMARY KEY ((hotel_id, start_date), room_number)
 * ) WITH comment = 'Q7. Find reservations by hotel and date';
*/
@Entity
@CqlName("reservations_by_hotel_date")
public class ReservationByHotelDateEntity extends AbstractReservationEntity {

    /** Serial. */
    private static final long serialVersionUID = -3392237616280919281L;
    
    public ReservationByHotelDateEntity() {}
    public ReservationByHotelDateEntity(Reservation r) { super(r); }
    
    @Override
    @PartitionKey(0)
    public String getHotelId() {
        return hotelId;
    }
    
    @Override
    @PartitionKey(1)
    public LocalDate getStartDate() {
        return startDate;
    }

    @Override
    @ClusteringColumn
    public Short getRoomNumber() {
        return roomNumber;
    }
    
}
