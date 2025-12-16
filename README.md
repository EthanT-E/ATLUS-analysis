# ATLAS-analysis
This is the docker compose version. to use Swarm switch and pull the Swarm branch
## prerequisites
- docker
- docker desktop (optional but recommended)
## How to use:
1) Pull the directory
2) Navigate to the ATLUS-analysis folder in the terminal
3) `docker compose up`
4) wait until deligator exit 0 message pops up
5) ctrl + c
6) view plot and results.txt in docker desktop -> volumes -> atlus-analysis-data-store
## Good to know
- Some times there is a bug if a rabbit network exists already
- if on `docker compose up` a message talking about swarm nodes pops up run `docker swarm init` then `docker compose up`
