package uk.co.agware.carpet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import uk.co.agware.carpet.change.tasks.ScriptTask;
import uk.co.agware.carpet.database.DatabaseConnector;

/**
 * Created by Philip Ward <Philip.Ward@agware.com> on 14/06/2016.
 */
public class TestScriptTask {

    private DatabaseConnector databaseConnector;

    @Before
    public void buildMocks(){
        databaseConnector = Mockito.mock(DatabaseConnector.class);
        Mockito.when(databaseConnector.executeStatement(Mockito.anyString())).thenReturn(true);
    }

    @Test
    public void testSimpleConstructor(){
        ScriptTask task = new ScriptTask("Task 1", 1, "SELECT * FROM Table", ";");
        Assert.assertEquals("Task 1", task.getTaskName());
        Assert.assertEquals(1, task.getTaskOrder());
        Assert.assertTrue(task.performTask(databaseConnector));
        Mockito.verify(databaseConnector, Mockito.times(1)).executeStatement("SELECT * FROM Table");
    }

    @Test
    public void testSimpleConstructorNoDelimiter(){
        ScriptTask task = new ScriptTask("Task 1", 1, "SELECT * FROM Table", "");
        Assert.assertEquals("Task 1", task.getTaskName());
        Assert.assertEquals(1, task.getTaskOrder());
        Assert.assertTrue(task.performTask(databaseConnector));
        Mockito.verify(databaseConnector, Mockito.times(1)).executeStatement("SELECT * FROM Table");
    }

    @Test
    public void testSimpleConstructorNullDelimiter(){
        ScriptTask task = new ScriptTask("Task 1", 1, "SELECT * FROM Table", null);
        Assert.assertEquals("Task 1", task.getTaskName());
        Assert.assertEquals(1, task.getTaskOrder());
        Assert.assertTrue(task.performTask(databaseConnector));
        Mockito.verify(databaseConnector, Mockito.times(1)).executeStatement("SELECT * FROM Table");
    }

    @Test
    public void testMultipleStatements(){
        StringBuilder script = new StringBuilder("SELECT * FROM Table;");
        script.append("SELECT * FROM another_table;");
        script.append("DELETE FROM TABLE");
        ScriptTask task = new ScriptTask("Task 1", 1, script.toString(), null);
        Assert.assertEquals("Task 1", task.getTaskName());
        Assert.assertEquals(1, task.getTaskOrder());
        Assert.assertTrue(task.performTask(databaseConnector));
        ArgumentCaptor<String> statements = ArgumentCaptor.forClass(String.class);
        Mockito.verify(databaseConnector, Mockito.times(3)).executeStatement(statements.capture());
        Assert.assertEquals(3, statements.getAllValues().size());
        Assert.assertTrue(statements.getAllValues().contains("SELECT * FROM Table"));
        Assert.assertTrue(statements.getAllValues().contains("SELECT * FROM another_table"));
        Assert.assertTrue(statements.getAllValues().contains("DELETE FROM TABLE"));
    }
}
