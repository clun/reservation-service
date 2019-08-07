package com.cassandraguide.mapper;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.UUID;

import com.cassandraguide.model.Reservation;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;

/**
 * For denormalization purpose some classes have the same fields.
 * 
 * Let's mutualize here.
 */
public class AbstractReservationEntity implements Serializable {

    /** Serial. */
    private static final long serialVersionUID = 2435317288519225994L;

    /** Confirmation for this Reservation. */
     @CqlName("confirmation_number")
    protected String confirmationNumber;
    
    /** Hotel identifier, as Text not param. */
    @CqlName("hotel_id")
    protected String hotelId;
    
    /** Formated as YYYY-MM-DD in interfaces. */
    @CqlName("start_date")
    protected LocalDate startDate;
    
    /** Room number. */
    @CqlName("room_number")
    protected Short roomNumber;
    
    /** Formated as YYYY-MM-DD in interfaces. */
    @CqlName("end_date")
    protected LocalDate endDate;
     
    /** UUID. */
    @CqlName("guest_id")
    protected UUID guestId;
    
    public AbstractReservationEntity() {}
    
    /**
     * Init using the higher level object sent by service.
     */
    public AbstractReservationEntity(Reservation bean) {
        this.hotelId            = bean.getHotelId();
        this.startDate          = bean.getStartDate();
        this.endDate            = bean.getEndDate();
        this.guestId            = bean.getGuestId();
        this.roomNumber         = bean.getRoomNumber();
        this.confirmationNumber = bean.getConfirmationNumber();
    }
    
    /** Mapping back to expected service bean. */
    public Reservation asReservation() {
        Reservation res = new Reservation();
        res.setConfirmationNumber(confirmationNumber);
        res.setStartDate(startDate);
        res.setEndDate(endDate);
        res.setGuestId(guestId);
        res.setHotelId(hotelId);
        if (getRoomNumber() != null) {
            res.setRoomNumber(roomNumber);
        }
        return res;
    }
    
    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "Confirmation Number = " + confirmationNumber +
                ", Hotel ID: " + getHotelId() +
                ", Start Date = " + getStartDate() +
                ", End Date = " + getEndDate() +
                ", Room Number = " + getRoomNumber() +
                ", Guest ID = " + getGuestId();
    }
    
    /**
     * Getter accessor for attribute 'confirmationNumber'.
     *
     * @return
     *       current value of 'confirmationNumber'
     */
    public String getConfirmationNumber() {
        return confirmationNumber;
    }

    /**
     * Setter accessor for attribute 'confirmationNumber'.
     * @param confirmationNumber
     *      new value for 'confirmationNumber '
     */
    public void setConfirmationNumber(String confirmationNumber) {
        this.confirmationNumber = confirmationNumber;
    }

    /**
     * Getter accessor for attribute 'hotelId'.
     *
     * @return
     *       current value of 'hotelId'
     */
    public String getHotelId() {
        return hotelId;
    }

    /**
     * Setter accessor for attribute 'hotelId'.
     * @param hotelId
     *      new value for 'hotelId '
     */
    public void setHotelId(String hotelId) {
        this.hotelId = hotelId;
    }

    /**
     * Getter accessor for attribute 'startDate'.
     *
     * @return
     *       current value of 'startDate'
     */
    public LocalDate getStartDate() {
        return startDate;
    }

    /**
     * Setter accessor for attribute 'startDate'.
     * @param startDate
     *      new value for 'startDate '
     */
    public void setStartDate(LocalDate startDate) {
        this.startDate = startDate;
    }

    /**
     * Getter accessor for attribute 'roomNumber'.
     *
     * @return
     *       current value of 'roomNumber'
     */
    public Short getRoomNumber() {
        return roomNumber;
    }

    /**
     * Setter accessor for attribute 'roomNumber'.
     * @param roomNumber
     *      new value for 'roomNumber '
     */
    public void setRoomNumber(Short roomNumber) {
        this.roomNumber = roomNumber;
    }

    /**
     * Getter accessor for attribute 'endDate'.
     *
     * @return
     *       current value of 'endDate'
     */
    public LocalDate getEndDate() {
        return endDate;
    }

    /**
     * Setter accessor for attribute 'endDate'.
     * @param endDate
     *      new value for 'endDate '
     */
    public void setEndDate(LocalDate endDate) {
        this.endDate = endDate;
    }

    /**
     * Getter accessor for attribute 'guestId'.
     *
     * @return
     *       current value of 'guestId'
     */
    public UUID getGuestId() {
        return guestId;
    }

    /**
     * Setter accessor for attribute 'guestId'.
     * @param guestId
     *      new value for 'guestId '
     */
    public void setGuestId(UUID guestId) {
        this.guestId = guestId;
    }
    
}
