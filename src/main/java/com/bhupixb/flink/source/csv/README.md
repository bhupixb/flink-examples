# Flink CSV Source Program

This program demonstrates reading data from a CSV file using Apache Flink.

## Prerequisites

- Java 11
- Intellij IDEA installed

## Instructions to run in IntelliJ IDEA

1. Clone or download this repository to your local machine.

2. Open Intellij IDEA and import the project by selecting the `pom.xml` file.

3. Go to the `CsvSourceMain` class in the project.

4. Right-click on the `main` method inside `CsvSourceMain` and select "Run CsvSourceMain.main()".

5. If prompted, edit the run configuration to include the necessary dependencies to the classpath:
    - Click on "Edit Configurations" from the top-right dropdown menu or the toolbar.
    - In the "Run/Debug Configurations" window, go to the "Dependencies" tab.
    - Add the required Flink dependencies (e.g., flink-core, flink-streaming-java, flink-clients, etc.) to the classpath.
    - Apply the changes and close the window.

6. Run the program by clicking the green "Run" button in the toolbar or pressing Shift+F10.

7. Monitor the program's output in the Intellij IDEA console.