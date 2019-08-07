package com.cassandraguide.repository.simple;

import com.cassandraguide.repository.AbstractReservationIntegrationTest;
import com.cassandraguide.repository.ReservationRepository;
import com.cassandraguide.repository.ReservationRepositorySimpleStatement;
import com.cassandraguide.repository.ReservationRepositoryWithQueryBuilder;

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
