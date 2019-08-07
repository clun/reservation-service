package com.cassandraguide.repository.mapper;

import com.cassandraguide.repository.AbstractReservationIntegrationTest;
import com.cassandraguide.repository.ReservationRepository;
import com.cassandraguide.repository.ReservationRepositoryWithMapper;

/**
 * Running integration tests on {@link ReservationRepositoryWithMapper}.
 */
public class WithMapper_IntegrationTest extends AbstractReservationIntegrationTest {

    /** {@inheritDoc} */
    @Override
    public ReservationRepository initReservationRepository() {
        return new ReservationRepositoryWithMapper(
                cassandraConfig.cqlSession(), 
                cassandraConfig.keyspace());
    }
    
}
