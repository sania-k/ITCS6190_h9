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
   Each task has a seperate directory, each filled with folders for each batch run. Each of these batch folders contain the output data, including the csv file. The csv file widata comtains different columns depending on the task.
   
   ### Task 1 Output Example:   
   ```bash
      trip_id,driver_id,distance_km,fare_amount,timestamp
      4a7b923d-1057-4c12-bf8d-cb89c900d955,64,6.43,67.06,2025-10-15 02:49:53
      f6b49a6c-2232-445e-b6d1-723b6f1c99b0,81,11.7,11.43,2025-10-15 02:49:54
   ```
   
   ### Task 2 Output Example:   
   ```bash
      driver_id,total_fare,avg_distance
      52,41.0,48.78
      47,10.26,5.3
      6,148.12,8.36
      90,188.98,33.83
      38,110.39,19.02
      58,179.09,39.78
      81,83.03,46.3
      24,68.88,5.59
      83,140.89,20.24
      14,33.61,15.99
      74,93.44,47.78
      76,101.71,8.72
   ```
   
   ### Task 3 Output Example:   
   ```bash
      window_start,window_end,sum_fare_amount
      2025-10-15T03:40:00.000Z,2025-10-15T03:45:00.000Z,1720.1999999999998
      2025-10-15T03:42:00.000Z,2025-10-15T03:47:00.000Z,1720.1999999999998
      2025-10-15T03:44:00.000Z,2025-10-15T03:49:00.000Z,1720.1999999999998
      2025-10-15T03:41:00.000Z,2025-10-15T03:46:00.000Z,1720.1999999999998
      2025-10-15T03:43:00.000Z,2025-10-15T03:48:00.000Z,1720.1999999999998
   ```

---

## **Reflection**
This assignment was fairly straightforward in implementation after going through documentation, abliet with some slight snags due to logic errors and output moves. Most of my issues came from my environment on my machine due to new Java and Python misconfigurations. I don't know why these have happened, but until I can reconfigure everything, I ran this on Codespaces.

