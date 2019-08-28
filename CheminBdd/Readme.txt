#Contenue du dossier
cheminbdd.py : Script
cheminbdd_env.yml : Permet de créer l'environnement avec les modules nécessaire au script
	On peut exporter l'environnement en utilisant le fichier sqoopy.yml et la commande suivante
 : `conda env create -f cheminbdd_env.yml`
Sortie : exemple de sortie

#Fonctionnalité
Permet de rechercher toutes les contraintes référentiels d'une table.
Recehrcher si il existe un chemins entre deux tables ou tables et colonnes de la base de données sans passer par des tables vide.
Génère automatiquement la requêtes de jointure si le chemins existe.


#Utilisation 
python cheminbdd.py --mode=voisin --tables DPLAN_INTERVENTION_PLANNING
python cheminbdd.py --mode=path --tables DPLAN_INTERVENTION_PLANNING PATIENT_ID  NOYAU_PATIENT PAT_ID
python cheminbdd.py --mode=join --tables DPLAN_INTERVENTION_PLANNING PATIENT_ID  NOYAU_PATIENT PAT_ID

