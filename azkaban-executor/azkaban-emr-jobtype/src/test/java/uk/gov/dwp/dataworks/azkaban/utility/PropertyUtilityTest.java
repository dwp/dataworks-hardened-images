package uk.gov.dwp.dataworks.azkaban.utility;

import azkaban.utils.Props;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.dwp.dataworks.azkaban.utility.PropertyUtility.COLLECTION_DEPENDENCIES_PARAMETER_NAME;
import static uk.gov.dwp.dataworks.azkaban.utility.PropertyUtility.EXPORT_DATE_PARAMETER_NAME;
import static uk.gov.dwp.dataworks.azkaban.utility.PropertyUtility.SKIP_NOTIFICATIONS_PARAMETER_NAME;

class PropertyUtilityTest {

    @Test
    public void shouldReturnSkipTrueWhenSetToTrue() {
        assertTrue(propertyUtility(SKIP_NOTIFICATIONS_PARAMETER_NAME, "true", "false").skipNotifications());
    }

    @Test
    public void shouldReturnSkipFalseWhenSetToFalse() {
        assertFalse(propertyUtility(SKIP_NOTIFICATIONS_PARAMETER_NAME, "false", "false").skipNotifications());
    }

    @Property
    public void shouldReturnSkipFalseWhenNotSetToTrue(@ForAll String value) {
        if (!value.equalsIgnoreCase("true")) {
            assertFalse(propertyUtility(SKIP_NOTIFICATIONS_PARAMETER_NAME, value, "false").skipNotifications());
        }
    }

    @ParameterizedTest
    @EmptySource
    public void shouldReturnNotSkipWhenEmpty(String value) {
        assertFalse(propertyUtility(SKIP_NOTIFICATIONS_PARAMETER_NAME, value, "false").skipNotifications());
    }

    @ParameterizedTest
    @ValueSource(strings = {"1,2,3", "1,2,3,", ",1,2,3", ",,,,1,2,3,,,,", ", , , , , , , 1 , 2 , 3    ,, ,, , "})
    public void shouldReturnMultipleDependenciesIfProvided(String value) {
        List<String> expected = Arrays.asList("1", "2", "3");
        assertLinesMatch(expected,
                propertyUtility(COLLECTION_DEPENDENCIES_PARAMETER_NAME, value, "").collectionDependencies());
    }

    @ParameterizedTest
    @ValueSource(strings = {"1", "1,", ",1", ",,,,1,,,,,", ", , , , , , , 1 ,  ,     ,, ,, , "})
    public void shouldReturnSingleDependencyIfProvided() {
        List<String> expected = Collections.singletonList("1");
        assertLinesMatch(expected,
                propertyUtility(COLLECTION_DEPENDENCIES_PARAMETER_NAME, "1", "").collectionDependencies());
    }

    @Test
    public void shouldReturnNoDependenciesIfNotProvided() {
        List<String> expected = Collections.emptyList();
        assertLinesMatch(expected,
                propertyUtility(COLLECTION_DEPENDENCIES_PARAMETER_NAME, "", "").collectionDependencies());
    }

    @Property
    public void shouldReturnSuppliedExportDate(@ForAll String value) {
        assertEquals(value, propertyUtility(EXPORT_DATE_PARAMETER_NAME, value, new SimpleDateFormat("yyyy-MM-dd").format(new Date())).exportDate());
    }

    @Test
    public void shouldReturnTodaysDateIfNotOverriden() {
        String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        assertEquals(today, propertyUtility(EXPORT_DATE_PARAMETER_NAME, today, today).exportDate());
    }

    private PropertyUtility propertyUtility(String parameterName, String parameterValue, String defaultValue) {
        Props jobProps = mock(Props.class);
        when(jobProps.getString(parameterName, defaultValue)).thenReturn(parameterValue);
        return new PropertyUtility(jobProps);
    }
}
