package com.cassandraguide.repository;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createType;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import com.cassandraguide.model.Reservation;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;

/**
 * Definition of functions expected in Data Access Object (DAO). We provide
 * 2 implementations:
 *   - Without Mapper but with QueryBuilder to help query builds
 *   - With Mapper 
 */
public interface ReservationRepository {
    
    /**
     * Reservation Keyspace constants (tables names, columns names)
     */
    CqlIdentifier TYPE_ADDRESS               = CqlIdentifier.fromCql("address");
    CqlIdentifier TABLE_RESERVATION_BY_HOTEL = CqlIdentifier.fromCql("reservations_by_hotel_date");
    CqlIdentifier TABLE_RESERVATION_BY_CONFI = CqlIdentifier.fromCql("reservations_by_confirmation");
    CqlIdentifier TABLE_RESERVATION_BY_GUEST = CqlIdentifier.fromCql("reservations_by_guest");
    CqlIdentifier TABLE_GUESTS               = CqlIdentifier.fromCql("guests");
    CqlIdentifier STREET                     = CqlIdentifier.fromCql("street");
    CqlIdentifier CITY                       = CqlIdentifier.fromCql("city");
    CqlIdentifier STATE_PROVINCE             = CqlIdentifier.fromCql("state_or_province");
    CqlIdentifier POSTAL_CODE                = CqlIdentifier.fromCql("postal_code");
    CqlIdentifier COUNTRY                    = CqlIdentifier.fromCql("country");
    CqlIdentifier HOTEL_ID                   = CqlIdentifier.fromCql("hotel_id");
    CqlIdentifier START_DATE                 = CqlIdentifier.fromCql("start_date");
    CqlIdentifier END_DATE                   = CqlIdentifier.fromCql("end_date");
    CqlIdentifier ROOM_NUMBER                = CqlIdentifier.fromCql("room_number");
    CqlIdentifier CONFIRMATION_NUMBER        = CqlIdentifier.fromCql("confirmation_number");
    CqlIdentifier GUEST_ID                   = CqlIdentifier.fromCql("guest_id");
    CqlIdentifier GUEST_LAST_NAME            = CqlIdentifier.fromCql("guest_last_name");
    CqlIdentifier FIRSTNAME                  = CqlIdentifier.fromCql("first_name");
    CqlIdentifier LASTNAME                   = CqlIdentifier.fromCql("last_name");
    CqlIdentifier TITLE                      = CqlIdentifier.fromCql("title");
    CqlIdentifier EMAILS                     = CqlIdentifier.fromCql("emails");
    CqlIdentifier PHONE_NUMBERS              = CqlIdentifier.fromCql("phone_numbers");
    CqlIdentifier ADDRESSES                  = CqlIdentifier.fromCql("addresses");
    
    /**
     * Testing existence is relevant to avoid mapping. To evaluate existence find the table 
     * where confirnation number is partition key which is reservations_by_confirmation
     * 
     * @param confirmationNumber
     *      unique identifier for confirmation
     * @return
     *      if the reservation exist or not
     */
    boolean exists(final String confirmationNumber);
    
    /**
     * Close from testing existence with Mapping and parsing of results.
     * 
     * @param confirmationNumber
     *      unique identifier for confirmation
     * @return
     *      reservation if present or empty
     */
    Optional<Reservation> findByConfirmationNumber(final String confirmationNumber);
    
    /**
     * Create new entry in multiple tables for this reservation.
     *
     * @param reservation
     *      current reservation object
     * @return
     *      
     */
     String upsert(Reservation reservation);
     
     /**
      * We pick 'reservations_by_confirmation' table to list reservations
      * BUT we could have used 'reservations_by_hotel_date' (as no key provided in request)
      *  
      * @returns
      *      list all reservations
      */
     List<Reservation> findAll();
     
     /**
      * Deleting a reservation. As not returned value why not switching to ASYNC.
      *
      * @param confirmationNumber
      *      unique identifier for confirmation.
      */
     void delete(String confirmationNumber);
     
     /**
      * Search all reservation for an hotel id and LocalDate.
      *
      * @param hotelId
      *      hotel identifier
      * @param date
      *      searched Date
      * @return
      */
      List<Reservation> findByHotelAndDate(String hotelId, LocalDate date);
      
      /**
       * Utility method to marshall a row as expected Reservation Bean.
       *
       * @param row
       *      current row fron ResultSet
       * @return
       *      object
       */
      default Reservation mapRowToReservation(Row row) {
         Reservation r = new Reservation();
         r.setHotelId(row.getString(HOTEL_ID));
         r.setConfirmationNumber(row.getString(CONFIRMATION_NUMBER));
         r.setGuestId(row.getUuid(GUEST_ID));
         r.setRoomNumber(row.getShort(ROOM_NUMBER));
         r.setStartDate(row.getLocalDate(START_DATE));
         r.setEndDate(row.getLocalDate(END_DATE));
         return r;
     }
      
     /**
      * Create Keyspace and relevant tables as per defined in 'reservation.cql'.
      *
      * @param cqlSession
      *          connectivity to Cassandra
      * @param keyspaceName
      *          keyspace name
      */
      default void createTables(CqlSession cqlSession, CqlIdentifier keyspaceName) {
         
         /**
          * Create TYPE 'Address' if not exists
          * 
          * CREATE TYPE reservation.address (
          *   street text,
          *   city text,
          *   state_or_province text,
          *   postal_code text,
          *   country text
          * );
          */
         cqlSession.execute(
                 createType(keyspaceName, TYPE_ADDRESS)
                 .ifNotExists()
                 .withField(STREET, DataTypes.TEXT)
                 .withField(CITY, DataTypes.TEXT)
                 .withField(STATE_PROVINCE, DataTypes.TEXT)
                 .withField(POSTAL_CODE, DataTypes.TEXT)
                 .withField(COUNTRY, DataTypes.TEXT)
                 .build());
         
         /** 
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
         cqlSession.execute(createTable(keyspaceName, TABLE_RESERVATION_BY_HOTEL)
                         .ifNotExists()
                         .withPartitionKey(HOTEL_ID, DataTypes.TEXT)
                         .withPartitionKey(START_DATE, DataTypes.DATE)
                         .withClusteringColumn(ROOM_NUMBER, DataTypes.SMALLINT)
                         .withColumn(END_DATE, DataTypes.DATE)
                         .withColumn(CONFIRMATION_NUMBER, DataTypes.TEXT)
                         .withColumn(GUEST_ID, DataTypes.UUID)
                         .withClusteringOrder(ROOM_NUMBER, ClusteringOrder.ASC)
                         .withComment("Q7. Find reservations by hotel and date")
                         .build());
         
         /**
          * CREATE TABLE reservation.reservations_by_confirmation (
          *   confirmation_number text PRIMARY KEY,
          *   hotel_id text,
          *   start_date date,
          *   end_date date,
          *   room_number smallint,
          *   guest_id uuid
          * );
          */
         cqlSession.execute(createTable(keyspaceName, TABLE_RESERVATION_BY_CONFI)
                 .ifNotExists()
                 .withPartitionKey(CONFIRMATION_NUMBER, DataTypes.TEXT)
                 .withColumn(HOTEL_ID, DataTypes.TEXT)
                 .withColumn(START_DATE, DataTypes.DATE)
                 .withColumn(END_DATE, DataTypes.DATE)
                 .withColumn(ROOM_NUMBER, DataTypes.SMALLINT)
                 .withColumn(GUEST_ID, DataTypes.UUID)
                 .build());
          
          /**
           * CREATE TABLE reservation.reservations_by_guest (
           *  guest_last_name text,
           *  hotel_id text,
           *  start_date date,
           *  end_date date,
           *  room_number smallint,
           *  confirmation_number text,
           *  guest_id uuid,
           *  PRIMARY KEY ((guest_last_name), hotel_id)
           * ) WITH comment = 'Q8. Find reservations by guest name';
           */
          cqlSession.execute(createTable(keyspaceName, TABLE_RESERVATION_BY_GUEST)
                  .ifNotExists()
                  .withPartitionKey(GUEST_LAST_NAME, DataTypes.TEXT)
                  .withClusteringColumn(HOTEL_ID, DataTypes.TEXT)
                  .withColumn(START_DATE, DataTypes.DATE)
                  .withColumn(END_DATE, DataTypes.DATE)
                  .withColumn(ROOM_NUMBER, DataTypes.SMALLINT)
                  .withColumn(CONFIRMATION_NUMBER, DataTypes.TEXT)
                  .withColumn(GUEST_ID, DataTypes.UUID)
                  .withComment("Q8. Find reservations by guest name")
                  .build());
           
           /**
            * CREATE TABLE reservation.guests (
            *   guest_id uuid PRIMARY KEY,
            *   first_name text,
            *   last_name text,
            *   title text,
            *   emails set<text>,
            *   phone_numbers list<text>,
            *   addresses map<text, frozen<address>>,
            *   confirmation_number text
            * ) WITH comment = 'Q9. Find guest by ID';
            */
           UserDefinedType  udtAddressType = 
                   cqlSession.getMetadata().getKeyspace(keyspaceName).get() // Retrieving KeySpaceMetadata
                             .getUserDefinedType(TYPE_ADDRESS).get();        // Looking for UDT (extending DataType)
           cqlSession.execute(createTable(keyspaceName, TABLE_GUESTS)
                   .ifNotExists()
                   .withPartitionKey(GUEST_ID, DataTypes.UUID)
                   .withColumn(FIRSTNAME, DataTypes.TEXT)
                   .withColumn(LASTNAME, DataTypes.TEXT)
                   .withColumn(TITLE, DataTypes.TEXT)
                   .withColumn(EMAILS, DataTypes.setOf(DataTypes.TEXT))
                   .withColumn(PHONE_NUMBERS, DataTypes.listOf(DataTypes.TEXT))
                   .withColumn(ADDRESSES, DataTypes.mapOf(DataTypes.TEXT, udtAddressType, true))
                   .withColumn(CONFIRMATION_NUMBER, DataTypes.TEXT)
                   .withComment("Q9. Find guest by ID")
                   .build());
     }
      
}
