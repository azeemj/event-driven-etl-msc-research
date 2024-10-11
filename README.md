# event-driven-etl
Event-Driven ETL Pipeline
Steps:

 1.Set up the ETL Data Pipeline

2. Utilize AWS Step Functions by deploying the provided state machine definition from AWS-Step-function/prototype-etl.json.
Build the Data Pipeline

3. Follow the architecture diagram to structure the pipeline components and their interactions effectively.
Develop Lambda Functions

4. Implement the following Lambda functions and integrate them within the Step Functions workflow:
DataSplit.py
Phase3-Data-Extraction.py
Phase3-Transformation.py
Phase3-DataLoad.py
Phase3-BackupData.py
RevertDataFromLoadErrorLambda.py
HandleDataExtractionErrorLambda.py
CloudsMatricsByQ2.py

5.Configure Permissions
  Ensure appropriate permissions and policies are in place for:
  S3 bucket access and policies
  AWS Lambda execution roles
  AWS Step Functions execution permissions
  Integrate Monitoring

6. Set up Grafana dashboards for visualization, log storage in S3, and monitoring via AWS CloudWatch.
7. Leverage DynamoDB for state or checkpoint management if needed.
8. Review Project Outputs

Refer to the project’s output images for additional implementation details and expected results.
 

![Screenshot 2024-10-11 104050](https://github.com/user-attachments/assets/826d536f-47dd-40de-a2a1-9dbd687b2275)



![Screenshot 2024-10-11 103849](https://github.com/user-attachments/assets/8d858885-d6bb-45f5-8108-80bef1577b5b)
![Screenshot 2024-10-11 104009](https://github.com/user-attachments/assets/5e9f71b8-debe-4fa5-b591-241b88725fc9)
![Screenshot 2024-10-11 103218](https://github.com/user-attachments/assets/9fba20bd-7cef-4635-98eb-c64c71ba82e4)
![Screenshot 2024-10-11 103241](https://github.com/user-attachments/assets/ed454bb9-84b5-4c2d-bc0a-c8f478e0a71c)
![Screenshot 2024-10-11 103253](https://github.com/user-attachments/assets/9bbd4527-0b95-4dcb-9cc0-d8acf5911a09)
