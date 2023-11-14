# T-DAT-901 PROJECT
## Structure & Architecture Big Data

## Installation des dépendances python

```
$ pip3 install -r requirement.txt
```

## Installation de docker & docker-compose

```
$ sudo apt update && sudo apt upgrade (Si nécessaire)
$ sudo apt install docker docker-compose (Si nécessaire)
```

## Installation & Exécution 
### Kafka, Zookeeper, Spark & Spark Worker via docker-compose

```
 $ sudo docker-compose up -d (Démarre tout les containers).
 $ sudo docker-compose up -d <NOM_SERVICE ex. cassandra> (Démarre uniquement cassandra).
```

## Création d'un topic Kafka pour produire et consommer la données

```
 $ sudo docker exec -it <CONTAINER_KAFKA_ID> kafka-topics --create --bootstrap-server <localhost>:<PORT_KAFKA ex.9092> --replication-factor 1 --partitions 1 --topic <NOM_TOPIC>
```

## Connexion Bash aux containers + gestion des containers

Listing des containers en cours d'éxécution :
```
$ sudo docker ps
```

Connexion à un container docker :
```
$ sudo docker exec -it <ID_CONTAINER OR NAME> /bin/bash
```

## Commande utiles Docker & Docker-compose

```
$ docker-compose up -d (Démarre et éxécute le fichier docker-compose.yml pour installer vos services, network, ...). (-d = Sans détails Mode Silently)
$ docker-compose stop (Stop simplement les services contenus dans un docker-compose.yml)
$ docker-compose down (Stop et Supprime les containers des services contenus dans le docker-compose.yml).
$ docker ps (Liste tout les containers dockers en cours d'éxécution).
$ docker ps --all (Liste tout les containers dockers allumé ou stoppé).
$ docker images (Liste toutes les images disponibles que vous avez pullez).
$ docker rm <CONTAINER_ID> (Supprime un container (Doit être éteint))
$ docker stop <CONTAINER_ID> (Stop un container actif pour sont ID).
$ docker rmi <IMAGES_IDS / ID> (Supprime un images par son ID ou NAME).
$ docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' <CONTAINER_ID> (Permet de récupérer IPV4 du container).
$ docker network ls (Liste tout les networks virtuels disponible sur votre docker).
```

## Commande utile SPARK Streaming

```
$ spark-submit --master spark://hostname:7077 --executor-cores 4 --executor-memory 4g --node worker-1 application.py

--master = Vers quel noeud master on souhaite éxécuter l'application.
--executor-cores = Combien de Coeur CPU on souhaite utiliser pour le traitement.
--executor-memory = Combien de mémoire vive (RAM) on souhaite utiliser pour le traitement.
--node = Le noeud que l'ont souhaite utiliser pour le traitement.
```

## Architectue et structure réseaux

```
Container KAFKA (172.18.0.5) ->  0.0.0.0:9092->9092/tcp, :::9092->9092/tcp  (Accessible via port 9092)
Container ZOOKEEPER (172.18.0.4) -> 2888/tcp, 0.0.0.0:2181->2181/tcp, :::2181->2181/tcp, 3888/tcp (Accessible via port 2181)
Container Spark Master (172.18.0.2) -> 0.0.0.0:7077->7077/tcp, :::7077->7077/tcp, 0.0.0.0:8080->8080/tcp, :::8080->8080/tcp  (Accessible via port 8080 & adresse localhost).
Container Spark Worker (172.18.0.3) -> Autobinding au Spark Master rien à faire. (Cluster). 
```


## Exécution du script en mode proccess background (VIA NOHUP)

```
$ nohup python3 scrapper-crypto.py (> output.log 2>&1 &  (POur avoir le l'output)) 
```


## Architecture et structure

```
.
├── Big-data-project  (Dossier global du projet)
│   ├── docker (Dossier contenant le fichier d'installation docker-compose)
│   │   └── docker-compose.yml
│   ├── drivers (Dossier contenant le drivers google pour Selenium Scrapper)
│   │   └── chromedriver
│   ├── kafka (Dossier contenant les fichiers de tests Kafka Producer & Consumer)
│   │   ├── kafka_consumer.py
│   │   └── kafka_producer.py
│   ├── requierement.txt (Fichier contenant les dépandances python du projet)
│   ├── scrapper (Fichier contenant le scrapper selenium pour récupérer le cour du bitcoin en continue)
│   │   └── scrapper-crypto.py
│   └── spark (Fichier contenant les Pipeline d'application spark a déploiyer sur le Spark Master pour distribution au workers)
│       ├── bitcoin_average.py
├── README.md 
└── TODO_LIST.txt (Fichier contenant des TODO's)

7 directories, 9 files
```