git clone <url>  
mkdir ./dags ./logs ./plugins  
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env   
chmod -R 777 ./logs  
sudo docker-compose up --build

  
 cure call: curl -X GET --user "airflow:airflow" "http://localhost:8080/api/v1/dags/s3_file_processor"
