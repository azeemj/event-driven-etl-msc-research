# event-driven-etl
Event driven ETL
Steps:
1. Setup ETL data pipline using AWS step functions which is avaibale AWS-Step-function/prototype-etl.json
2. Build the data pipline based on the architecture diagram
3. BUild the Follwong Lmbdas and connect them with AWS lambda functions
4. Make sure S3 bucket permissions /Policies , AWS Lamnda permissions , AWS step functions permissoins
5. Files are : DataSplit.py, Phase3-Data-Extraction.py, Phase3-Transformation.py, Phase3-DataLoad.py, Phase3-BackupData.py, RevertDataFromLoadErrorLambda.py, HandleDataExtractionErrorLambda.py, CloudsMatricsByQ2.py, RevertDataFromLoadErrorLambda.py
6.Make sure to have setup Grafana , AWS logs on S3 , DynamoDB
7. Check project output images for more details 

![Screenshot 2024-10-11 104050](https://github.com/user-attachments/assets/826d536f-47dd-40de-a2a1-9dbd687b2275)



![Screenshot 2024-10-11 103849](https://github.com/user-attachments/assets/8d858885-d6bb-45f5-8108-80bef1577b5b)
![Screenshot 2024-10-11 104009](https://github.com/user-attachments/assets/5e9f71b8-debe-4fa5-b591-241b88725fc9)
![Screenshot 2024-10-11 103218](https://github.com/user-attachments/assets/9fba20bd-7cef-4635-98eb-c64c71ba82e4)
![Screenshot 2024-10-11 103241](https://github.com/user-attachments/assets/ed454bb9-84b5-4c2d-bc0a-c8f478e0a71c)
![Screenshot 2024-10-11 103253](https://github.com/user-attachments/assets/9bbd4527-0b95-4dcb-9cc0-d8acf5911a09)
