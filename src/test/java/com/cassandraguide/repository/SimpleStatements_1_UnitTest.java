package com.cassandraguide.repository;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Testing repository with Cassandra Connectivity Mocked
 */
@ExtendWith(MockitoExtension.class)
public class SimpleStatements_1_UnitTest extends AbstractReservationUnitTest {

    /** {@inheritDoc} */
    @Override
    protected ReservationRepository initReservationRepository() {
        return new ReservationRepositorySimpleStatement(cqlSession, keyspaceName, true);
    }
    
}
