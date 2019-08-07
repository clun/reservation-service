package com.cassandraguide.mapper;

import com.cassandraguide.model.Reservation;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;

/**
 * This class will help us generating a Request to insert 
 * in multiple tables using batch.
 */
public class ReservationDaoHelper {
    
    private final CqlSession cqlSession;
    private final EntityHelper<ReservationByConfirmationEntity> resaByConfirmationHelper;
    private final EntityHelper<ReservationByHotelDateEntity>    resaByHotelDateHelper;
    
    private PreparedStatement psInsertReservationByHotelDate;
    private PreparedStatement psDeleteReservationByHotelDate;
    
    private PreparedStatement psInsertReservationByConfirmation;
    private PreparedStatement psDeleteReservationByConfirmation;
    
    public ReservationDaoHelper(MapperContext context,
            EntityHelper<ReservationByHotelDateEntity> resaHotelDate,
            EntityHelper<ReservationByConfirmationEntity> resaConfirm) {
        this.cqlSession               = context.getSession();
        this.resaByConfirmationHelper = resaConfirm;
        this.resaByHotelDateHelper    = resaHotelDate;
        this.psInsertReservationByHotelDate    = 
                cqlSession.prepare(resaByHotelDateHelper.insert().asCql());
        this.psDeleteReservationByHotelDate    = 
                cqlSession.prepare(resaByHotelDateHelper.deleteByPrimaryKey().asCql());
        this.psInsertReservationByConfirmation = 
                cqlSession.prepare(resaByConfirmationHelper.insert().asCql());
        this.psDeleteReservationByConfirmation    = 
                cqlSession.prepare(resaByConfirmationHelper.deleteByPrimaryKey().asCql());
    }
    
    /**
     * Same signature as the Dao providing explicit queries and batch.
     *
     * @param res
     *          current reservation to process
     */
    public void upsertWithQueryProvider(Reservation res) {
        cqlSession.execute(
            BatchStatement.builder(DefaultBatchType.LOGGED)
                .addStatement(bind(psInsertReservationByHotelDate, 
                                    new ReservationByHotelDateEntity(res), 
                                    resaByHotelDateHelper))
                .addStatement(bind(psInsertReservationByConfirmation, 
                                    new ReservationByConfirmationEntity(res), 
                                    resaByConfirmationHelper))
                .build());
    }
    
    void deleteReservation(Reservation res) {
        
       // Mapping only attributes for the PK
       ReservationByHotelDateEntity r1 = new ReservationByHotelDateEntity();
       r1.setHotelId(res.getHotelId());
       r1.setRoomNumber(res.getRoomNumber());
       r1.setStartDate(res.getStartDate());
       
       ReservationByConfirmationEntity r2 = new ReservationByConfirmationEntity();
       r2.setConfirmationNumber(res.getConfirmationNumber());
       
       cqlSession.execute(
            BatchStatement.builder(DefaultBatchType.LOGGED)
                .addStatement(bind(psDeleteReservationByHotelDate, r1,resaByHotelDateHelper))
                .addStatement(bind(psDeleteReservationByConfirmation, r2, resaByConfirmationHelper))
                .build());
    }
    
    public static <T> BoundStatement bind(PreparedStatement preparedStatement, T entity, EntityHelper<T> entityHelper) {
        BoundStatementBuilder boundStatement = preparedStatement.boundStatementBuilder();
        entityHelper.set(entity, boundStatement, NullSavingStrategy.DO_NOT_SET);
        return boundStatement.build();
    }

}
