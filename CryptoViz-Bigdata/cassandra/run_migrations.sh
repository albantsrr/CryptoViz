
#!/bin/bash

# Définition des paramètres de connexion à Cassandra
HOST="localhost"
PORT="9042"
KEYSPACE="bitcoin_data"

# Chemin vers le répertoire de migrations
MIGRATIONS_DIR="migrations"

# Exécuter chaque fichier CQL dans l'ordre
for migration in $(ls $MIGRATIONS_DIR | sort)
do
    echo "Exécution de la migration: $migration"
    cqlsh $HOST $PORT -k $KEYSPACE -f "$MIGRATIONS_DIR/$migration"
    if [ $? -ne 0 ]; then
        echo "Erreur lors de l'exécution de la migration: $migration"
        exit 1
    fi
done

echo "Toutes les migrations ont été appliquées avec succès."

