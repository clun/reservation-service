package com.cassandraguide.repository;

/**
 * Running integration tests on {@link ReservationRepositoryWithQueryBuilder}.
 */
public class WithQueryBuilder_2_IntegrationTest extends AbstractReservationIntegrationTest {

    /** {@inheritDoc} */
    @Override
    public ReservationRepository initReservationRepository() {
        return new ReservationRepositoryWithQueryBuilder(
                cassandraConfig.cqlSession(), 
                cassandraConfig.keyspace());
    }

}
