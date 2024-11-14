
# Transvision Star Monitoring Engine

**IMPORTANT** - This repository is for internal use only!

#### The scenarios which is a pivotal component of the STAR methodology for identifying and combatting money laundering. These scenarios serve as the cornerstone in our efforts to detect and prevent illicit financial activities.



### For clearer comprehension and visualization, please refer to the provided image depicting the flow of the scenario pipeline - 

![Pipeline-Page-1 drawio (1)](https://github.com/user-attachments/assets/593648d1-2e1f-44e1-badf-b58df84648fb)


<hr>

## Steps to Run the Project:
1. Open the terminal in the parent directory containing your "requirements.txt" file and execute the following command:

    ```bash
    pip install -r requirements.txt
    ```

2. Open the terminal in the parent directory containing your "runner.py" file and execute the following command:

    ```bash
    python runner.py
    ```

    The above command initiates a Flask endpoint accessible at http://127.0.0.1:5000/api/execute_pipeline.
   
3. Open Postman or a similar tool and use the structured request provided below with the necessary details. Then, send a POST request to the specified endpoint. Feel free to use any tool that is similar to Postman for this task.

    ```bash
    {
      "jobID": 184,
      "jobNM": "High Risk Geography",
      "scenarioID": ["TS_SCN_01"],
      "trigerBY": "DS_TEAM",
      "isManual": 1
    }
    ```
4. Once scenario executed successfully you will get a response message "Pipeline executed successfully" with status - "success" and the successfully executed scenario ids as list - ["TS_SCN_43"].

    ```bash
    {
      "message": "Pipeline executed successfully",
      "status": "success",
      "success_scenario_ids": [
          "TS_SCN_43"
      ],
    "isManual": 1
    }
    ```

<hr>



