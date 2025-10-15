# Ride Sharing Analytics Using Spark Streaming and Spark SQL.
---
## **Overview**

In this assignment, we will build a real-time analytics pipeline for a ride-sharing platform using Apache Spark Structured Streaming. we will process streaming data, perform real-time aggregations, and analyze trends over time.

---

## **Prerequisites**
Before running, ensure you have the following software installed and properly configured on your machine:
1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Faker**:
   - Install using `pip`:
     ```bash
     pip install faker
     ```

---
## Project Structure**

```
ride-sharing-analytics/
├── outputs/
│   ├── task_1
│   |    └── CSV files of task 1.
|   ├── task_2
│   |    └── CSV files of task 2.
|   └── task_3
│       └── CSV files of task 3.
├── task1.py
├── task2.py
├── task3.py
├── data_generator.py
└── README.md
```

- **data_generator.py/**: generates a constant stream of input data of the schema (trip_id, driver_id, distance_km, fare_amount, timestamp)  
- **task_1**: Ingests streaming data from socket and parses into JSON message before outputting to console and csv
- **task_2**: Aggregates incoming steaming data to calculate total fare amount and agerage distance, outputs to csv
- **task_3**:  Converts incoming streaming data into timestap and performs 5-minute windowed aggregation on fare amount, outputs to csv
- **outputs/**: CSV files of processed data of each task stored in respective folders.
- **README.md**: Assignment instructions and guidelines.
  
---

## Running the Analysis Tasks**

You can run the analysis tasks either locally.

1. **Execute Each Task **: The data_generator.py should be continuosly running on a terminal. open a new terminal to execute each of the tasks.
   ```bash
     python data_generator.py
   ```
   ```bash
     python task1.py
   ```
   ```bash
     python task2.py
   ```
   ```bash
     python task3.py
   ```

2. **Verify the Outputs**:
   Check the `outputs/` directory for the resulting files:
   - `task_1/batch_0/*.csv`
   - `task_2/batch_0/*.csv`
   - `task_3/batch_0/*.csv`
   Each task has a seperate directory, each filled with folders for each batch run. Each of these batch folders contain the output data, including the csv file. The csv file will contain data like this:

   
   ```bash
   trip_id,driver_id,distance_km,fare_amount,timestamp,event_time
   8d076a3f-12d2-4f92-a1a0-52adc8ac6486,83,32.6,91.77,2025-10-15 02:26:01,2025-10-15T02:26:01.000Z
   ```

---

## **Reflection**
This assignment was fairly straightforward in implementation after going through documentation, however I started to have issues with my environment and Java and Python misconfigurations. I don't know why these have happened but until I can reconfigure everything, I ran this on Codespaces.

