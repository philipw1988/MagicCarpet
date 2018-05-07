# MagicCarpet!

The Magic Carpet is a fun game played in a field with a group of guys who aren't fully aware of their own mortality, the basic idea is that you tow a piece of carpet around a field behind a tractor, on the carpet are a group of people who then fight to see who is the last man standing. Shockingly there have been no recorded instances of major injuries from this activity, however it was retired quite a few years ago now, but the legend lives on.

## Purpose

This library is intended as an auto updating tool for the application database which will be triggered when the application is started, there are any number of ways to configure how it is run, however there is one example shown below.

## Configuration

The application is configured with an XML Document (Example Below) which describes a version number, and that version then haa a set of tasks inside it for performing the actual updates. This allows for incremental database updates while doing development work, rather than having to apply them manually until release time. The execution order of the tasks can be defined with an attribute, otherwise the tasks are run in the order they appear after the document has been parsed, therefore it is a good idea to add explicit ordering if it is needed.

When creating the MagicCarpet instance, you can also pass in an optional boolean to disable it running even if some other code executes its run method at some later time.

## Database Access

The database connection used will need to have write access to the database given, it will also create a new table to track which changes it has applied to, currently this will be written to the same schema as the connection.

## Usage

Using this library is at your own risk, we are not responsible for any loss of data or database corruption caused by using this code for your own projects, remember to carefully test all changes before deploying the application anywhere but dev environments and always back up your database.

The example below uses Spring to show one way to use the application, there are other and maybe better ways to do it:

Create a MagicCarpet Bean, the "devMode" flag is optional here, this also calls the .run() method within the Bean creation before returning the Bean, this will run all the updates first
```
@Bean(name = "magic-carpet")
public MagicCarpet magicCarpet() throws SQLException {
    DatabaseConnector databaseConnector = new DefaultDatabaseConnector();
    databaseConnector.setConnection(portalDataSource().getConnection());
    MagicCarpet magicCarpet = new MagicCarpet(databaseConnector, devMode);
    magicCarpet.run();
    return magicCarpet;
}
```

In your configuration files, make sure that any Database Access class Beans, such as an EntityManager for JPA or JDBCTemplate for Spring JDBC are annotated with ``@DependsOn("magic-carpet")``, this will ensure that they are created after the Magic Carpet Bean has completed and therefore nothing else should be attempting to access the Database before the updates have been completed.

You can also do more manual processes if you want to double check that the changes have been loaded up correctly, the ``MagicCarpet.run()`` method runs the ``MagicCarpet.parseChanges();`` and then ``MagicCarpet.executeChanges();`` methods for you, however if you wish you can run ``MagicCarpet.parseChanges();`` and then check the number of changes that have been loaded and if you wish even manually execute a single change or a list of changes using the ``DatabaseConnector.executeChanges(List<Change>)`` or ``DatabaseConnector.executeChange(Change)`` methods. 

## ChangeSet.xml

By default, MagicCarpet will search the classpath for a ChangeSet.xml file, however it is possible to override this with a file from a different location if you choose:

Path object that points to the file:
```
magicCarpet.setChangeSetFile(Paths.get("src/test/java/ChangeSet.xml"));
```

InputStream that points to the file:
```
magicCarpet.setChangeSetFile(new FileInputStream(new File("src/test/java/ChangeSet.xml")));
```

Inside the ChangeSet.xml is where the definitions for all the changes you want to create sit:
```
<changeList>
    <change id="1.0.0" delimiter=";">
        <task name="Create DB">
            <script>
                create table test(id integer, test date);
                alter table test add column another varchar(64);
                create table second(id varchar(64))
            </script>
        </task>
    </change>
    <change id="1.0.1">
        <task name="Fix Bug">
            <file>classpath:changes/1.0.1.sql</file>
        </task>
    </change>
    <change id="1.1.0">
        <task name="Add New Table" order="1">
            <file>classpath:addNewTable.sql</file>
        </task
        <task name="Alter Table" order="2">
            <file>classpath:alterTable.sql</file>
        </task
    </change>
</changeList>
```
Each "change" can contain any number of tasks to be executed, there are a number of rules that determine the execution order and validity of a task, they are the following:

* Each change element is executed in ascending order of "id", that means that 1.0.0 is before 1.0.1, which is before 1.1.0. The application is expecting versions of the form integer.integer.integer
* Each change element needs to have a unique ID
* Each Task within a change needs to have a unique name for that change. Repeat names are allowed between changes, but not within them.
* Tasks are executed in the order they appear unless otherwise stated. If some tasks contain an order and some do not then the ones with an order are executed first in the order they resolve to.

##### Delimiter
Each task can have a custom delimiter for when there are multiple statements to execute. By default the delimiter is set to semi colon.
