package com.cassandraguide.repository;

/**
 * Running integration tests on {@link ReservationRepositoryWithMapper}.
 */
public class WithMapper_2_IntegrationTest extends AbstractReservationIntegrationTest {

    /** {@inheritDoc} */
    @Override
    public ReservationRepository initReservationRepository() {
        return new ReservationRepositoryWithMapper(
                cassandraConfig.cqlSession(), 
                cassandraConfig.keyspace());
    }
    
}
