package com.cassandraguide.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

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

/**
 * Testing repository with Cassandra Connectivity Mocked
 */
@ExtendWith(MockitoExtension.class)
public class WithQueryBuilder_1_UnitTest {
    
    @Mock 
    private CqlSession cqlSession;
    @Mock
    private CqlIdentifier keyspaceName;
    @Mock
    private PreparedStatement psExistReservation;
    @Mock
    private PreparedStatement psFindReservation;
    @Mock
    private PreparedStatement psInsertReservationByHotel;
    @Mock
    private PreparedStatement psInsertReservationByConfirmation;
    @Mock
    private PreparedStatement psDeleteReservation;
    @Mock
    private PreparedStatement psSearchReservation;
    @Mock
    private BoundStatement mockBound;
    @Mock
    private ResultSet mockResultSet;
    
    private ReservationRepositoryWithQueryBuilder testedRepository;
    
    @BeforeEach
    public void _init() {
        testedRepository = new ReservationRepositoryWithQueryBuilder(cqlSession, keyspaceName,
                psExistReservation, psFindReservation, psInsertReservationByHotel,
                psInsertReservationByConfirmation, psDeleteReservation, psSearchReservation);
    }
    
    @Test
    @DisplayName("Confirmation is required to evaluate reservation existence")
    public void existReservation_should_throw_if_null_confirmationnumber() {
        assertThatThrownBy(() -> { testedRepository.exists(null);})
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("should not be null");
    }
    
    @Test
    @DisplayName("Return true if reservation exist with the confirmation number")
    public void existReservation_should_return_true_if_reservation_exists() {
        // GIVEN : Tell Mockito expect behaviour for PreparedStatement,BoundStatement,ResultSet
        given(psExistReservation.bind("10")).willReturn(mockBound);
        given(cqlSession.execute(mockBound)).willReturn(mockResultSet);
        given(mockResultSet.getAvailableWithoutFetching()).willReturn(1);
        // WHEN THEN
        assertThat(testedRepository.exists("10")).isTrue();
        verify(cqlSession).execute(mockBound);
    }
    
    @Test
    @DisplayName("Return false if reservation does not exist with the confirmation number")
    public void existReservation_should_return_false_if_reservation_doesnot_exist() {
        // GIVEN : Tell Mockito expect behaviour for PreparedStatement,BoundStatement,ResultSet
        given(psExistReservation.bind("11")).willReturn(mockBound);
        given(cqlSession.execute(mockBound)).willReturn(mockResultSet);
        given(mockResultSet.getAvailableWithoutFetching()).willReturn(0);
        // WHEN THEN
        assertThat(testedRepository.exists("11")).isFalse();
        verify(cqlSession).execute(mockBound);
    }

}
