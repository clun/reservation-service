package com.cassandraguide.repository;

/**
 * Running integration tests on {@link ReservationRepositoryWithQueryBuilder}.
 */
public class SimpleStatements_2_IntegrationTest extends AbstractReservationIntegrationTest {

    /** {@inheritDoc} */
    @Override
    public ReservationRepository initReservationRepository() {
        return new ReservationRepositorySimpleStatement(
                cassandraConfig.cqlSession(), 
                cassandraConfig.keyspace());
    }

}
