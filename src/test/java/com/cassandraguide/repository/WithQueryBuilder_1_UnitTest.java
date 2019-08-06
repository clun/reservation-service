package com.cassandraguide.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Testing repository with Cassandra Connectivity Mocked.
 * 
 * All test logic has been moved in the parent class to test the same behaviour with the
 * 3 implementations.
 */
@ExtendWith(MockitoExtension.class)
public class WithQueryBuilder_1_UnitTest extends AbstractReservationUnitTest {
    
    /** {@inheritDoc} */
    @Override
    protected ReservationRepository initReservationRepository() {
        return new ReservationRepositoryWithQueryBuilder(
                cqlSession, keyspaceName, 
                psExistReservation,
                psFindReservation,
                psInsertReservationByHotel,
                psInsertReservationByConfirmation,
                psDeleteReservation,
                psDeleteReservationConfirmation,
                psSearchReservation);
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
