# ATLAS-analysis
This is the Swarm version. to use Compose switch and pull the main branch
## prerequisites
- docker
- docker desktop (optional but recommended)
## How to use:
1) Pull the directory
2) Navigate to the ATLUS-analysis folder in the terminal
3) `docker swarm init`
4) `docker stack deploy -c docker-compose.yaml atlus`
5) view plot and results.txt in docker desktop -> volumes -> atlus-data-store
6) Wait for files to be written into the volume
7) `docker swarm leave --force`
