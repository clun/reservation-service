package com.cassandraguide.repository.simple;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.cassandraguide.repository.AbstractReservationUnitTest;
import com.cassandraguide.repository.ReservationRepository;
import com.cassandraguide.repository.ReservationRepositorySimpleStatement;

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
