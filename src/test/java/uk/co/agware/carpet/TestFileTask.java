package uk.co.agware.carpet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import uk.co.agware.carpet.change.tasks.FileTask;
import uk.co.agware.carpet.database.DatabaseConnector;

/**
 * Created by Philip Ward <Philip.Ward@agware.com> on 14/06/2016.
 */
public class TestFileTask {

    private DatabaseConnector databaseConnector;

    @Before
    public void buildMocks(){
        databaseConnector = Mockito.mock(DatabaseConnector.class);
        Mockito.when(databaseConnector.executeStatement(Mockito.anyString())).thenReturn(true);
    }

    @Test
    public void testAbsoluteFilePath(){
        FileTask task = new FileTask("Task 1", 1, "src/test/files/test.sql", ";");
        Assert.assertEquals("Task 1", task.getTaskName());
        Assert.assertEquals(1, task.getTaskOrder());
        Assert.assertTrue(task.performTask(databaseConnector));
        ArgumentCaptor<String> statements = ArgumentCaptor.forClass(String.class);
        Mockito.verify(databaseConnector, Mockito.times(2)).executeStatement(statements.capture());
        Assert.assertEquals(2, statements.getAllValues().size());
        Assert.assertTrue(statements.getAllValues().contains("SELECT * FROM Table"));
        Assert.assertTrue(statements.getAllValues().contains("SELECT * FROM Other_Table"));
    }

    @Test
    public void testAbsoluteFilePathNoDelimiter(){
        FileTask task = new FileTask("Task 1", 1, "src/test/files/test.sql", "");
        Assert.assertEquals("Task 1", task.getTaskName());
        Assert.assertEquals(1, task.getTaskOrder());
        Assert.assertTrue(task.performTask(databaseConnector));
        ArgumentCaptor<String> statements = ArgumentCaptor.forClass(String.class);
        Mockito.verify(databaseConnector, Mockito.times(2)).executeStatement(statements.capture());
        Assert.assertEquals(2, statements.getAllValues().size());
        Assert.assertTrue(statements.getAllValues().contains("SELECT * FROM Table"));
        Assert.assertTrue(statements.getAllValues().contains("SELECT * FROM Other_Table"));
    }

    @Test
    public void testAbsoluteFilePathNullDelimiter(){
        FileTask task = new FileTask("Task 1", 1, "src/test/files/test.sql", null);
        Assert.assertEquals("Task 1", task.getTaskName());
        Assert.assertEquals(1, task.getTaskOrder());
        Assert.assertTrue(task.performTask(databaseConnector));
        ArgumentCaptor<String> statements = ArgumentCaptor.forClass(String.class);
        Mockito.verify(databaseConnector, Mockito.times(2)).executeStatement(statements.capture());
        Assert.assertEquals(2, statements.getAllValues().size());
        Assert.assertTrue(statements.getAllValues().contains("SELECT * FROM Table"));
        Assert.assertTrue(statements.getAllValues().contains("SELECT * FROM Other_Table"));
    }

    @Test
    public void testClasspathFile(){
        FileTask task = new FileTask("Task 2", 2, "classpath:classpathTest.sql", ","); // Testing a random delimiter as well
        Assert.assertEquals("Task 2", task.getTaskName());
        Assert.assertEquals(2, task.getTaskOrder());
        Assert.assertTrue(task.performTask(databaseConnector));
        ArgumentCaptor<String> statements = ArgumentCaptor.forClass(String.class);
        Mockito.verify(databaseConnector, Mockito.times(2)).executeStatement(statements.capture());
        Assert.assertEquals(2, statements.getAllValues().size());
        Assert.assertTrue(statements.getAllValues().contains("SELECT * FROM Classpath"));
        Assert.assertTrue(statements.getAllValues().contains("SELECT * FROM class_path"));
    }
}