package com.cassandraguide.repository.querybuilder;

import com.cassandraguide.repository.AbstractReservationIntegrationTest;
import com.cassandraguide.repository.ReservationRepository;
import com.cassandraguide.repository.ReservationRepositoryWithQueryBuilder;

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
