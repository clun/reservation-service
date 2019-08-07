package com.cassandraguide.repository;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;

@ExtendWith(MockitoExtension.class)
public abstract class AbstractReservationUnitTest {
    
    @Mock 
    protected CqlSession cqlSession;
    @Mock
    protected CqlIdentifier keyspaceName;
    @Mock
    protected PreparedStatement psExistReservation;
    @Mock
    protected PreparedStatement psFindReservation;
    @Mock
    protected PreparedStatement psInsertReservationByHotel;
    @Mock
    protected PreparedStatement psInsertReservationByConfirmation;
    @Mock
    protected PreparedStatement psDeleteReservation;
    @Mock
    protected PreparedStatement psDeleteReservationConfirmation;
    @Mock
    protected PreparedStatement psSearchReservation;
    @Mock
    protected BoundStatement mockBound;
    @Mock
    protected ResultSet mockResultSet;
    
    /** To be implemented by sub classes. */
    protected abstract ReservationRepository initReservationRepository();
    
    protected ReservationRepository testedRepository = null;
    
    /*
     * ReCreate keyspace and table before each test
     */
    @BeforeEach
    public void _init() {
        testedRepository = initReservationRepository();
    }
    
    @Test
    @DisplayName("Confirmation is required to evaluate reservation existence")
    public void existReservation_should_throw_if_null_confirmationnumber() {
        assertThatThrownBy(() -> { testedRepository.exists(null);})
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("should not be null");
    }
    
}
