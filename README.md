# snapshot-service
Snapshot Service

docker network create -d bridge snapshot 
docker network ls  

docker build -t gar2000b/snapshot-service .  
docker run -it -d -p 9087:9087 --network="snapshot" --name snapshot-service gar2000b/snapshot-service  

All optional:

docker create -it gar2000b/snapshot-service bash  
docker ps -a  
docker start ####  
docker ps  
docker attach ####  
docker remove ####  
docker image rm gar2000b/snapshot-service  
docker exec -it snapshot-service sh  
docker login  
docker push gar2000b/snapshot-service  